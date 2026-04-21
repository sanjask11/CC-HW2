#!/usr/bin/env python3
import json
import logging
import mimetypes
import os
import re
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import unquote, urlparse

from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET = os.environ.get("BUCKET", "")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PORT = int(os.environ.get("PORT", "8080"))
TOPIC = os.environ.get("TOPIC", "forbidden-requests")

BANNED = {
    "north korea",
    "iran",
    "cuba",
    "myanmar",
    "iraq",
    "libya",
    "sudan",
    "zimbabwe",
    "syria",
    "korea, democratic people's republic of",
    "democratic people's republic of korea",
    "korea (democratic people's republic of)",
    "dprk",
}

SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")

cloud_logging.Client().setup_logging()
log = logging.getLogger("hw9.service1")
log.setLevel(logging.INFO)

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient() if (PROJECT_ID and TOPIC) else None
topic_path = publisher.topic_path(PROJECT_ID, TOPIC) if publisher else None


def get_client_country(headers) -> str:
    for key in ("X-Country", "X-Client-Country", "X-Geo-Country", "CF-IPCountry"):
        value = headers.get(key)
        if value:
            return value.strip()
    return "Unknown"


def publish_forbidden(payload: dict) -> None:
    if not publisher or not topic_path:
        return
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    publisher.publish(topic_path, data=data)


def blob_name_for_path(path: str) -> str:
    path = path.lstrip("/")
    return f"{PAGES_PREFIX}/{path}" if PAGES_PREFIX else path


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    server_version = "hw9-service1/1.0"

    def send_with_headers(self, code: int, body: bytes, content_type: str = "text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def warn(self, msg: str, **fields):
        log.warning(msg, extra={"json_fields": fields})

    def critical(self, msg: str, **fields):
        log.critical(msg, extra={"json_fields": fields})

    def info(self, msg: str, **fields):
        log.info(msg, extra={"json_fields": fields})

    def do_GET(self):
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)

        # Dedicated health endpoint for GKE probes
        if raw_path == "/healthz":
            self.send_with_headers(200, b"ok\n")
            return

        if raw_path in ("", "/"):
            raw_path = "/0.html"

        if ".." in raw_path or not SAFE_PATH_RE.match(raw_path.lstrip("/")):
            self.warn(
                "Invalid path",
                path=raw_path,
                method="GET",
                client_ip=self.client_address[0],
            )
            self.send_with_headers(404, b"not found\n")
            return

        country = get_client_country(self.headers)
        client_ip = self.client_address[0]

        if country.lower() in BANNED:
            payload = {
                "ts": int(time.time()),
                "country": country,
                "client_ip": client_ip,
                "path": raw_path,
                "method": "GET",
            }
            self.critical("Forbidden country request", **payload)
            try:
                publish_forbidden(payload)
            except Exception:
                log.exception("Failed to publish forbidden request")

            self.send_with_headers(403, b"forbidden\n")
            return
        blob_name = blob_name_for_path(raw_path)

        try:
            bucket = storage_client.bucket(BUCKET)
            blob = bucket.blob(blob_name)

            if not blob.exists(timeout=10):
                self.warn(
                    "File not found",
                    bucket=BUCKET,
                    blob=blob_name,
                    path=raw_path,
                    method="GET",
                    country=country,
                    client_ip=client_ip,
                )
                self.send_with_headers(404, b"not found\n")
                return

            data = blob.download_as_bytes(timeout=30)
            ctype, _ = mimetypes.guess_type(blob_name)
            if not ctype:
                ctype = "application/octet-stream"

            self.info(
                "Served file",
                bucket=BUCKET,
                blob=blob_name,
                bytes=len(data),
                country=country,
                client_ip=client_ip,
            )
            self.send_with_headers(200, data, content_type=ctype)
        except Exception as exc:
            log.exception("Server error")
            self.send_with_headers(500, f"server error: {exc}\n".encode("utf-8"))

    def not_implemented(self):
        country = get_client_country(self.headers)
        self.warn(
            "Method not implemented",
            method=self.command,
            path=self.path,
            country=country,
            client_ip=self.client_address[0],
        )
        self.send_with_headers(501, b"not implemented\n")

    def do_POST(self): self.not_implemented()
    def do_PUT(self): self.not_implemented()
    def do_DELETE(self): self.not_implemented()
    def do_HEAD(self): self.not_implemented()
    def do_CONNECT(self): self.not_implemented()
    def do_OPTIONS(self): self.not_implemented()
    def do_TRACE(self): self.not_implemented()
    def do_PATCH(self): self.not_implemented()

    def log_message(self, fmt, *args):
        return


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info(
        "Listening",
        extra={"json_fields": {"port": PORT, "bucket": BUCKET, "topic": TOPIC}},
    )
    server.serve_forever()


if __name__ == "__main__":
    main()