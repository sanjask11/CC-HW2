import json
import os
from datetime import datetime, timezone

from flask import Flask, Response, request
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging



BUCKET = os.environ.get("BUCKET", "san-hw2-cc")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PUBSUB_TOPIC = os.environ.get("TOPIC", "forbidden-requests")


FORBIDDEN_COUNTRIES = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"
}

app = Flask(__name__)


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
logging_client = cloud_logging.Client()
logger = logging_client.logger("hw4-service1")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _project_id() -> str:
    return os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT", "")


def _topic_path() -> str:
    pid = _project_id()
    if not pid:
        raise RuntimeError("Missing GOOGLE_CLOUD_PROJECT / GCLOUD_PROJECT for Pub/Sub topic path.")
    return publisher.topic_path(pid, PUBSUB_TOPIC)


def _normalize_object_path(url_path: str) -> str:
    url_path = url_path.lstrip("/")
    if not url_path:
        return f"{PAGES_PREFIX}/"
    if url_path.startswith(f"{PAGES_PREFIX}/"):
        return url_path
    return f"{PAGES_PREFIX}/{url_path}"


def _log_struct(severity: str, payload: dict):
    logger.log_struct(payload, severity=severity)


def _publish_forbidden(event: dict):
    data = json.dumps(event).encode("utf-8")
    publisher.publish(_topic_path(), data=data)


@app.route("/", methods=["GET"])
def root_get():
    return Response("OK\n", status=200, mimetype="text/plain")


@app.route("/<path:req_path>", methods=["GET"])
def serve_file(req_path: str):
    country = request.headers.get("X-country", "").strip()
    method = request.method
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

    if country in FORBIDDEN_COUNTRIES:
        event = {
            "timestamp": _utc_now_iso(),
            "country": country,
            "path": f"/{req_path}",
            "method": method,
            "client_ip": client_ip,
            "bucket": BUCKET,
            "topic": PUBSUB_TOPIC,
            "reason": "forbidden_country",
        }
        _log_struct("CRITICAL", {"status": 400, **event})
        try:
            _publish_forbidden(event)
        except Exception as e:
            _log_struct("ERROR", {
                "status": 400,
                "error": f"pubsub_publish_failed: {type(e).__name__}: {str(e)}",
                **event
            })
        return Response("Permission denied\n", status=400, mimetype="text/plain")

    obj_path = _normalize_object_path(req_path)
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(obj_path)

    try:
        if not blob.exists():
            _log_struct("WARNING", {
                "timestamp": _utc_now_iso(),
                "status": 404,
                "path": f"/{req_path}",
                "object": obj_path,
                "method": method,
                "client_ip": client_ip,
                "country": country,
                "reason": "not_found",
            })
            return Response("Not Found\n", status=404, mimetype="text/plain")

        data = blob.download_as_bytes()
        return Response(data, status=200, mimetype="text/html")
    except Exception as e:
        _log_struct("ERROR", {
            "timestamp": _utc_now_iso(),
            "status": 500,
            "path": f"/{req_path}",
            "object": obj_path,
            "method": method,
            "client_ip": client_ip,
            "country": country,
            "error": f"{type(e).__name__}: {str(e)}",
        })
        return Response("Internal Server Error\n", status=500, mimetype="text/plain")


@app.route("/<path:req_path>", methods=[
    "POST", "PUT", "DELETE", "HEAD", "CONNECT", "OPTIONS", "TRACE", "PATCH"
])
def not_implemented(req_path: str):
    country = request.headers.get("X-country", "").strip()
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

    _log_struct("WARNING", {
        "timestamp": _utc_now_iso(),
        "status": 501,
        "path": f"/{req_path}",
        "method": request.method,
        "client_ip": client_ip,
        "country": country,
        "reason": "not_implemented",
    })
    return Response("Not Implemented\n", status=501, mimetype="text/plain")
