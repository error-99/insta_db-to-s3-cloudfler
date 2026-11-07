"""
MemoriesBangla cron worker
- /cron    : webcron hits this -> fetch pending -> RapidAPI -> upload to Cloudflare R2 -> save back to api.php
- /health  : API + R2 storage status
- /        : ok
"""
import os
import time
import fcntl
import logging
import mimetypes
import threading
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


# ============================== STEP 3: download -> R2 ==============================
def upload_to_r2(item_id, download_url, shortcode):
    with requests.get(
        download_url, stream=True, timeout=(15, 120), headers={"User-Agent": UA}
    ) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True

        ctype = (resp.headers.get("Content-Type") or "video/mp4").split(";")[0].strip()
        ext = (mimetypes.guess_extension(ctype) or ".mp4").lstrip(".")
        if ext == "jpe":
            ext = "jpg"

        key = f"memories/{item_id}_{shortcode or int(time.time())}.{ext}"
        r2_client().upload_fileobj(
            resp.raw,
            CFG["R2_BUCKET"],
            key,
            ExtraArgs={"ContentType": ctype},
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
    insta_url = item["url"]
    log.info(f"[{item_id}] start {insta_url}")

    meta = fetch_rapid(insta_url)

    # Save caption (only if API returned one and DB has none)
    caption = (meta.get("caption") or "").strip()
    if caption and not (item.get("caption") or "").strip():
        post_update({"id": item_id, "caption": caption})
        log.info(f"[{item_id}] caption saved")

    # Upload media to R2
    public_url = upload_to_r2(item_id, meta["download_url"], meta.get("id"))
    log.info(f"[{item_id}] uploaded {public_url}")

    # Save Cloudflare URL LAST so failed items are retried next run
    post_update({"id": item_id, "cloudflermediaurl": public_url})
    log.info(f"[{item_id}] done")
    return public_url


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
        pending = get_pending()
        for item in pending[: CFG["BATCH_SIZE"]]:
            try:
                url = process_item(item)
                results.append({"id": item["id"], "ok": True, "url": url})
            except Exception as e:
                log.exception(f"[{item.get('id')}] failed")
                results.append({"id": item.get("id"), "ok": False, "error": str(e)})
        return {"status": "success", "pending": len(pending), "processed": results}
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
