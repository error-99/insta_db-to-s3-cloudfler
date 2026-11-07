"""
MemoriesBangla cron worker
- /cron    : webcron hits this -> fetch pending -> RapidAPI -> upload to Cloudflare R2 -> save back to api.php
- /health  : API + R2 storage status
- /        : ok
"""
import os
import json
import time
import fcntl
import logging
import hashlib
import threading
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv
from flask import Flask, request, jsonify, abort

load_dotenv()  # reads .env locally; on Render env vars are used directly

# ============================== CONFIG ==============================
CFG = {
    # Your PHP API
    "API_BASE": os.getenv("API_BASE", "https://shabbir.serv00.net/Fbtools/memoriesbangla/api.php"),
    # Optional: if you add a token check on the PHP side, it is sent as a header
    "API_TOKEN": os.getenv("API_TOKEN", ""),

    # RapidAPI
    "RAPID_API_KEY": os.getenv("RAPID_API_KEY", ""),
    "RAPID_API_HOST": os.getenv(
        "RAPID_API_HOST",
        "instagram-downloader-download-instagram-videos-stories1.p.rapidapi.com",
    ),

    # Cloudflare R2
    "R2_ACCOUNT_ID": os.getenv("R2_ACCOUNT_ID", ""),
    "R2_ACCESS_KEY": os.getenv("R2_ACCESS_KEY", ""),
    "R2_SECRET_KEY": os.getenv("R2_SECRET_KEY", ""),
    "R2_BUCKET": os.getenv("R2_BUCKET", ""),
    "R2_PUBLIC_BASE": os.getenv("R2_PUBLIC_BASE", "").rstrip("/"),

    # Protects /cron and detailed /health
    "CRON_SECRET": os.getenv("CRON_SECRET", ""),

    # Items processed per cron hit
    "BATCH_SIZE": int(os.getenv("BATCH_SIZE", "2")),

    # Retry: try each task this many times, then skip it
    "MAX_RETRIES": int(os.getenv("MAX_RETRIES", "3")),
    "RETRY_DELAY": int(os.getenv("RETRY_DELAY", "3")),  # seconds (x attempt number)
}

LOCK_FILE = "/tmp/memoriesbangla_cron.lock"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cron")
app = Flask(__name__)


# ============================== HELPERS ==============================
def api_headers():
    h = {"Content-Type": "application/json"}
    if CFG["API_TOKEN"]:
        h["X-API-Token"] = CFG["API_TOKEN"]
    return h


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{CFG['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=CFG["R2_ACCESS_KEY"],
        aws_secret_access_key=CFG["R2_SECRET_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def human(n):
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} PB"


def authed():
    return bool(CFG["CRON_SECRET"]) and request.args.get("key") == CFG["CRON_SECRET"]


# ============================== STEP 1: pending list ==============================
def get_pending():
    r = requests.get(f"{CFG['API_BASE']}?pending_category", timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "success":
        raise RuntimeError(f"Pending API error: {j}")
    return j.get("data", [])


# ============================== STEP 2: RapidAPI ==============================
def fetch_rapid(insta_url):
    r = requests.get(
        f"https://{CFG['RAPID_API_HOST']}/fetch",
        params={"url": insta_url},
        headers={
            "x-rapidapi-host": CFG["RAPID_API_HOST"],
            "x-rapidapi-key": CFG["RAPID_API_KEY"],
        },
        timeout=40,
    )
    r.raise_for_status()
    j = r.json()
    if not j.get("ok") or not j.get("download_url"):
        raise RuntimeError(f"RapidAPI no download_url: {str(j)[:300]}")
    return j


# ============================== STEP 3: download -> R2 (original quality) ==============================
IG_HOSTS = ("instagram.com", "instagr.am")
IG_PATHS = ("/reel/", "/reels/", "/p/", "/tv/", "/stories/")

EXT_MAP = {
    "video/mp4": "mp4", "video/quicktime": "mov", "video/webm": "webm",
    "video/x-matroska": "mkv", "video/3gpp": "3gp",
    "image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
    "image/webp": "webp", "image/gif": "gif", "image/heic": "heic",
    "image/avif": "avif",
    "audio/mpeg": "mp3", "audio/mp4": "m4a", "audio/aac": "aac",
    "audio/ogg": "ogg", "audio/wav": "wav", "audio/x-wav": "wav",
    "application/pdf": "pdf",
}


def is_instagram_page(url):
    """True only for Instagram post/reel/story pages (these need RapidAPI)."""
    p = urlparse(url)
    host = (p.hostname or "").lower()
    on_ig = any(host == h or host.endswith("." + h) for h in IG_HOSTS)
    return on_ig and any(seg in p.path for seg in IG_PATHS)


def _to_int(v):
    try:
        return int(float(v))
    except Exception:
        return 0


def pick_best_url(meta):
    """Highest quality URL. If RapidAPI returns several variants, pick the biggest."""
    best_url, best_score = meta.get("download_url"), -1
    for key in ("medias", "media", "formats", "links", "variants", "videos", "items"):
        lst = meta.get(key)
        if not isinstance(lst, list):
            continue
        for m in lst:
            if not isinstance(m, dict):
                continue
            u = m.get("download_url") or m.get("url")
            if not u:
                continue
            score = (_to_int(m.get("width")) * _to_int(m.get("height"))) \
                or _to_int(m.get("bitrate")) or _to_int(m.get("size"))
            if score > best_score:
                best_url, best_score = u, score
    return best_url


def name_hint_from_url(url):
    """asset_id for lookaside links, else short hash."""
    q = parse_qs(urlparse(url).query)
    if q.get("asset_id"):
        return q["asset_id"][0]
    return hashlib.sha1(url.encode()).hexdigest()[:12]


def stream_to_r2(item_id, src_url, hint):
    """Stream the ORIGINAL bytes to R2 (no re-encoding = no quality loss)."""
    with requests.get(
        src_url, stream=True, timeout=(15, 180), headers={"User-Agent": UA}
    ) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True

        ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype.startswith("text/") or ctype in ("application/json", "application/xml"):
            raise RuntimeError(f"Link is not media (got {ctype}); it may be expired")

        ext = EXT_MAP.get(ctype)
        if not ext:  # fallback: extension in URL path
            path_ext = os.path.splitext(urlparse(src_url).path)[1].lstrip(".").lower()
            ext = path_ext if 1 <= len(path_ext) <= 5 else "bin"
        if not ctype or ctype == "application/octet-stream":
            ctype = {"mp4": "video/mp4", "jpg": "image/jpeg", "png": "image/png"}.get(
                ext, "application/octet-stream"
            )

        key = f"memories/{item_id}_{hint or int(time.time())}.{ext}"
        r2_client().upload_fileobj(
            resp.raw,
            CFG["R2_BUCKET"],
            key,
            ExtraArgs={
                "ContentType": ctype,
                "CacheControl": "public, max-age=31536000",
            },
        )
    return f"{CFG['R2_PUBLIC_BASE']}/{key}"


# ============================== STEP 4: save back to PHP API ==============================
def post_update(payload):
    r = requests.post(CFG["API_BASE"], json=payload, headers=api_headers(), timeout=30)
    r.raise_for_status()
    return r.text


# ============================== PROCESS ONE ITEM ==============================
def process_item(item):
    item_id = item["id"]
    url = (item.get("url") or "").strip()
    log.info(f"[{item_id}] start {url[:100]}")

    if is_instagram_page(url):
        # Instagram reel/post link -> RapidAPI
        meta = fetch_rapid(url)

        caption = (meta.get("caption") or "").strip()
        if caption and not (item.get("caption") or "").strip():
            post_update({"id": item_id, "caption": caption})
            log.info(f"[{item_id}] caption saved")

        src_url = pick_best_url(meta)
        hint = meta.get("id")
        log.info(f"[{item_id}] source: rapidapi")
    else:
        # Direct media link (lookaside.fbsbx.com, CDN, image, etc.) -> download directly
        src_url = url
        hint = name_hint_from_url(url)
        log.info(f"[{item_id}] source: direct link")

    public_url = stream_to_r2(item_id, src_url, hint)
    log.info(f"[{item_id}] uploaded {public_url}")

    # Save Cloudflare URL LAST so failed items are retried next run
    post_update({"id": item_id, "cloudflermediaurl": public_url})
    log.info(f"[{item_id}] done")
    return public_url


# ============================== SKIPPED TASKS (stored in R2, survives restarts) ==============================
SKIP_KEY = "_state/skipped.json"


def load_skipped():
    try:
        obj = r2_client().get_object(Bucket=CFG["R2_BUCKET"], Key=SKIP_KEY)
        return json.loads(obj["Body"].read())
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            return {}
        raise


def save_skipped(data):
    r2_client().put_object(
        Bucket=CFG["R2_BUCKET"],
        Key=SKIP_KEY,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )


def is_skipped(item, skipped):
    rec = skipped.get(str(item["id"]))
    # if the URL was changed in the DB, give it a fresh start
    return bool(rec) and rec.get("url") == item.get("url")


# ============================== RETRY WRAPPER ==============================
def process_with_retry(item):
    """Try up to MAX_RETRIES times. If all fail, report skipped=True."""
    max_try = CFG["MAX_RETRIES"]
    last_err = ""
    for attempt in range(1, max_try + 1):
        try:
            url = process_item(item)
            return {"id": item["id"], "ok": True, "url": url, "attempts": attempt}
        except Exception as e:
            last_err = str(e)[:300]
            log.warning(f"[{item.get('id')}] attempt {attempt}/{max_try} failed: {last_err}")
            if attempt < max_try:
                time.sleep(CFG["RETRY_DELAY"] * attempt)
    log.error(f"[{item.get('id')}] FAILED {max_try} times -> skipped")
    return {"id": item["id"], "ok": False, "skipped": True,
            "attempts": max_try, "error": last_err}


# ============================== BATCH RUNNER ==============================
def run_batch():
    """Runs one batch under a file lock. Returns a result dict."""
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        return {"status": "busy", "message": "previous run still going"}

    results = []
    try:
        skipped = load_skipped()
        pending = get_pending()
        todo = [i for i in pending if not is_skipped(i, skipped)]
        changed = False

        for item in todo[: CFG["BATCH_SIZE"]]:
            res = process_with_retry(item)
            results.append(res)
            sid = str(item["id"])
            if res["ok"]:
                if skipped.pop(sid, None) is not None:
                    changed = True
            else:
                skipped[sid] = {
                    "url": item.get("url"),
                    "error": res["error"],
                    "attempts": res["attempts"],
                    "time": datetime.now(timezone.utc).isoformat(),
                }
                changed = True

        if changed:
            save_skipped(skipped)

        return {
            "status": "success",
            "pending": len(pending),
            "waiting": max(len(todo) - CFG["BATCH_SIZE"], 0),
            "skipped_total": len(skipped),
            "processed": results,
        }
    except Exception as e:
        log.exception("batch failed")
        return {"status": "error", "error": str(e)}
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


# ============================== ROUTES ==============================
@app.route("/")
def home():
    return "ok"


@app.route("/cron")
def cron():
    """
    /cron?key=SECRET          -> starts in background, replies instantly (best for Render free tier)
    /cron?key=SECRET&wait=1   -> runs now and returns full results
    """
    if not authed():
        abort(403)

    if request.args.get("wait") == "1":
        res = run_batch()
        return jsonify(res), (500 if res.get("status") == "error" else 200)

    threading.Thread(target=run_batch, daemon=True).start()
    return jsonify(status="started")


@app.route("/skipped")
def skipped_list():
    """List tasks skipped after all retries failed."""
    if not authed():
        abort(403)
    data = load_skipped()
    return jsonify(count=len(data), skipped=data)


@app.route("/retry")
def retry_skipped():
    """/retry?key=SECRET&id=5  or  /retry?key=SECRET&all=1  -> put skipped task(s) back in the queue."""
    if not authed():
        abort(403)
    data = load_skipped()
    if request.args.get("all") == "1":
        n = len(data)
        data = {}
    else:
        rid = request.args.get("id", "")
        n = 1 if data.pop(rid, None) is not None else 0
    save_skipped(data)
    return jsonify(status="ok", requeued=n, skipped_left=len(data))


# ============================== HEALTH ==============================
def check_api():
    t = time.time()
    try:
        r = requests.get(f"{CFG['API_BASE']}?pending_category", timeout=15)
        j = r.json()
        return {
            "ok": r.status_code == 200 and j.get("status") == "success",
            "http_code": r.status_code,
            "response_ms": round((time.time() - t) * 1000),
            "pending_count": j.get("count"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def check_storage(deep=False):
    t = time.time()
    try:
        s3 = r2_client()
        bucket = CFG["R2_BUCKET"]
        s3.head_bucket(Bucket=bucket)  # connection + permission

        total_objs, total_size, latest = 0, 0, None
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, PaginationConfig={"PageSize": 1000, "MaxItems": 10000}
        )
        for page in pages:
            for o in page.get("Contents", []):
                total_objs += 1
                total_size += o["Size"]
                if latest is None or o["LastModified"] > latest["LastModified"]:
                    latest = o
        capped = total_objs >= 10000

        info = {
            "ok": True,
            "response_ms": round((time.time() - t) * 1000),
            "bucket": bucket,
            "endpoint": f"{CFG['R2_ACCOUNT_ID'][:6]}...r2.cloudflarestorage.com",
            "public_base": CFG["R2_PUBLIC_BASE"],
            "object_count": f"{total_objs}+" if capped else total_objs,
            "total_size": human(total_size) + ("+" if capped else ""),
            "total_size_bytes": total_size,
            "latest_object": {
                "key": latest["Key"],
                "size": human(latest["Size"]),
                "modified": latest["LastModified"].astimezone(timezone.utc).isoformat(),
                "url": f"{CFG['R2_PUBLIC_BASE']}/{latest['Key']}",
            }
            if latest
            else None,
        }

        if deep:  # write / read / delete test
            k = "_health/test.txt"
            s3.put_object(Bucket=bucket, Key=k, Body=b"ok")
            body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
            s3.delete_object(Bucket=bucket, Key=k)
            info["write_read_delete"] = body == b"ok"
            if body != b"ok":
                info["ok"] = False

        return info
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def check_config():
    needed = {
        "RAPID_API_KEY": CFG["RAPID_API_KEY"],
        "R2_ACCOUNT_ID": CFG["R2_ACCOUNT_ID"],
        "R2_ACCESS_KEY": CFG["R2_ACCESS_KEY"],
        "R2_SECRET_KEY": CFG["R2_SECRET_KEY"],
        "R2_BUCKET": CFG["R2_BUCKET"],
        "R2_PUBLIC_BASE": CFG["R2_PUBLIC_BASE"],
        "CRON_SECRET": CFG["CRON_SECRET"],
    }
    missing = [k for k, v in needed.items() if not v]
    return {"ok": not missing, "missing": missing}


@app.route("/health")
def health():
    is_auth = authed()
    deep = request.args.get("deep") == "1" and is_auth

    api = check_api()
    storage = check_storage(deep=deep)
    config = check_config()
    all_ok = api["ok"] and storage["ok"] and config["ok"]
    code = 200 if all_ok else 503

    try:
        skipped_count = len(load_skipped()) if (is_auth and storage["ok"]) else None
    except Exception:
        skipped_count = None

    if not is_auth:  # public: simple status only
        return (
            jsonify(
                status="healthy" if all_ok else "unhealthy",
                api=api["ok"],
                storage=storage["ok"],
            ),
            code,
        )

    return (
        jsonify(
            status="healthy" if all_ok else "unhealthy",
            time=datetime.now(timezone.utc).isoformat(),
            api=api,
            storage=storage,
            config=config,
            skipped_count=skipped_count,
        ),
        code,
    )


# ============================== LOCAL RUN ==============================
# python app.py        -> start web server
# python app.py run    -> process one batch (for server cron jobs)
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "run":
        print(run_batch())
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
