import json
import os
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import pubsub_v1

FORBIDDEN = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq",
    "Libya", "Sudan", "Zimbabwe", "Syria"
}

PROJECT_ID = (
    os.environ.get("GOOGLE_CLOUD_PROJECT")
    or os.environ.get("GCP_PROJECT")
    or os.environ.get("GCLOUD_PROJECT")
    or os.environ.get("PROJECT_ID")
)
if not PROJECT_ID:
    PROJECT_ID = ""

BUCKET = os.environ.get("BUCKET") or os.environ.get("BUCKET_NAME")
if not BUCKET:
    raise RuntimeError("Missing BUCKET / BUCKET_NAME env var")

PAGES_PREFIX = (os.environ.get("PAGES_PREFIX") or "html-pages").strip("/")
TOPIC = os.environ.get("TOPIC")
if not TOPIC:
    raise RuntimeError("Missing TOPIC env var")

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
def _topic_path():
    pid = PROJECT_ID or (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or os.environ.get("PROJECT_ID")
        or ""
    )
    if not pid:
        raise RuntimeError("Missing project id at runtime (set GOOGLE_CLOUD_PROJECT)")
    return publisher.topic_path(pid, TOPIC)


def _log(event_type: str, **fields):
    payload = {
        "event_type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    print(json.dumps(payload))


def _client_ip(request) -> str:
    xff = (request.headers.get("X-Forwarded-For", "") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr


def _extract_filename(request) -> str:
    # Case 1: query parameter ?file=0.html
    filename = (request.args.get("file", "") or "").strip()
    if filename:
        return filename

    # Case 2: path-based request /html-pages/0.html
    path = (getattr(request, "path", "") or "").lstrip("/")
    if not path:
        return ""

    parts = [p for p in path.split("/") if p]
    return parts[-1]


def serve(request):
    method = (request.method or "").upper()

    if method != "GET":
        _log("not_implemented", method=method, path=getattr(request, "path", ""))
        return ("Not Implemented\n", 501, {"Content-Type": "text/plain; charset=utf-8"})

    filename = _extract_filename(request)
    filename = filename.lstrip("/")
    if filename.startswith(PAGES_PREFIX + "/"):
        filename = filename[len(PAGES_PREFIX) + 1:]

    country = (request.headers.get("X-country", "") or "").strip()
    if country in FORBIDDEN:
        msg = {
            "reason": "forbidden_country",
            "country": country,
            "file": filename,
            "path": getattr(request, "path", ""),
            "remote_addr": _client_ip(request),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        publisher.publish(_topic_path(), json.dumps(msg).encode("utf-8")).result(timeout=5)
        _log("forbidden_country", country=country, file=msg["file"], remote_addr=msg["remote_addr"])
        return ("Permission denied\n", 400, {"Content-Type": "text/plain; charset=utf-8"})

    if not filename:
        _log("bad_request", reason="missing_file_param")
        return ("Missing file name\n", 400, {"Content-Type": "text/plain; charset=utf-8"})

    object_name = f"{PAGES_PREFIX}/{filename}".lstrip("/")

    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(object_name)

    if not blob.exists(storage_client):
        _log("not_found", file=filename, object=object_name)
        return ("Not Found\n", 404, {"Content-Type": "text/plain; charset=utf-8"})

    data = blob.download_as_bytes()
    _log("ok", file=filename, object=object_name, size=len(data))
    return (data, 200, {"Content-Type": "text/html; charset=utf-8"})
