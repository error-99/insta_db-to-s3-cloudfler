import os, re, json, time, fcntl, logging
from urllib.parse import urlencode

import requests
import boto3
from botocore.config import Config
from flask import Flask, request, jsonify, abort

# ================== CONFIG (use env vars or edit here) ==================
CFG = {
    # Your PHP API
    "API_BASE": "https://shabbir.serv00.net/Fbtools/memoriesbangla/api.php",

    # RapidAPI
    "RAPID_API_KEY": os.getenv("RAPID_API_KEY", "PUT_NEW_KEY_HERE"),
    "RAPID_API_HOST": "instagram-downloader-download-instagram-videos-stories1.p.rapidapi.com",

    # Cloudflare R2
    "R2_ACCOUNT_ID": os.getenv("R2_ACCOUNT_ID", "YOUR_ACCOUNT_ID"),
    "R2_ACCESS_KEY": os.getenv("R2_ACCESS_KEY", "YOUR_R2_ACCESS_KEY"),
    "R2_SECRET_KEY": os.getenv("R2_SECRET_KEY", "YOUR_R2_SECRET_KEY"),
    "R2_BUCKET": os.getenv("R2_BUCKET", "your-bucket"),
    # Public bucket URL (r2.dev or custom domain), no trailing slash
    "R2_PUBLIC_BASE": os.getenv("R2_PUBLIC_BASE", "https://pub-xxxx.r2.dev"),

    # Webcron protection: call /cron?key=THIS_SECRET
    "CRON_SECRET": os.getenv("CRON_SECRET", "change-me-long-random"),

    # How many items to process per hit (keep small to avoid webcron timeout)
    "BATCH_SIZE": 2,
}

LOCK_FILE = "/tmp/memoriesbangla_cron.lock"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("cron")
app = Flask(__name__)


# ================== R2 CLIENT ==================
def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{CFG['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=CFG["R2_ACCESS_KEY"],
        aws_secret_access_key=CFG["R2_SECRET_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


# ================== STEP 1: pending list ==================
def get_pending():
    r = requests.get(f"{CFG['API_BASE']}?pending_category", timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "success":
        raise RuntimeError(f"Pending API error: {j}")
    return j.get("data", [])


# ================== STEP 2: RapidAPI ==================
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


# ================== STEP 3: download -> R2 ==================
def upload_to_r2(item_id, download_url, shortcode):
    ext = "mp4"
    key = f"memories/{item_id}_{shortcode or int(time.time())}.{ext}"

    with requests.get(download_url, stream=True, timeout=(15, 120),
                      headers={"User-Agent": UA}) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True
        r2_client().upload_fileobj(
            resp.raw,
            CFG["R2_BUCKET"],
            key,
            ExtraArgs={"ContentType": "video/mp4"},
        )
    return f"{CFG['R2_PUBLIC_BASE']}/{key}"


# ================== STEP 4: save back to your PHP API ==================
def post_update(payload):
    r = requests.post(CFG["API_BASE"], json=payload, timeout=30)
    r.raise_for_status()
    return r.text


# ================== PROCESS ONE ITEM ==================
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

    # Upload video to R2
    public_url = upload_to_r2(item_id, meta["download_url"], meta.get("id"))
    log.info(f"[{item_id}] uploaded {public_url}")

    # Save cloudflare url LAST (so failed items are retried next run)
    post_update({"id": item_id, "cloudflermediaurl": public_url})
    log.info(f"[{item_id}] done")
    return public_url


# ================== CRON ENDPOINT ==================
@app.route("/cron")
def cron():
    if request.args.get("key") != CFG["CRON_SECRET"]:
        abort(403)

    # prevent overlapping runs
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return jsonify(status="busy", message="previous run still going")

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
        return jsonify(status="success", pending=len(pending), processed=results)
    except Exception as e:
        log.exception("cron failed")
        return jsonify(status="error", error=str(e)), 500
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


@app.route("/")
def home():
    return "ok"


# Run directly from a server cron job:  python3 app.py
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        for it in get_pending()[: CFG["BATCH_SIZE"]]:
            try:
                process_item(it)
            except Exception:
                log.exception("failed")
    else:
        app.run(host="0.0.0.0", port=5000)
