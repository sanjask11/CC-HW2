#!/usr/bin/env python3
import logging
import mimetypes
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import unquote, urlparse

from google.cloud import logging as cloud_logging
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET = os.environ.get("BUCKET", "")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PORT = int(os.environ.get("PORT", "8080"))
ZONE_NAME = os.environ.get("ZONE_NAME", "unknown-zone")

SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")

cloud_logging.Client().setup_logging()
log = logging.getLogger("hw8.service1")
log.setLevel(logging.INFO)

storage_client = storage.Client()


def blob_name_for_path(path: str) -> str:
    if path in ("", "/"):
        path = "index.html"
    path = path.lstrip("/")
    return f"{PAGES_PREFIX}/{path}" if PAGES_PREFIX else path


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    server_version = "hw8-service1/1.0"

    def send_with_headers(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Zone", ZONE_NAME)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def warn(self, msg: str, **fields):
        log.warning(msg, extra={"json_fields": fields})

    def info(self, msg: str, **fields):
        log.info(msg, extra={"json_fields": fields})

    def do_GET(self):
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)

        # Health check endpoint for LB
        if raw_path == "/healthz":
            self.send_with_headers(200, b"ok\n", "text/plain; charset=utf-8")
            return

        if ".." in raw_path or not SAFE_PATH_RE.match(raw_path.lstrip("/")):
            self.warn(
                "Invalid path",
                path=raw_path,
                client_ip=self.client_address[0],
                zone=ZONE_NAME,
            )
            self.send_with_headers(404, b"not found\n", "text/plain; charset=utf-8")
            return

        blob_name = blob_name_for_path(raw_path)

        try:
            bucket = storage_client.bucket(BUCKET)
            blob = bucket.blob(blob_name)

            if not blob.exists():
                self.warn(
                    "File not found",
                    bucket=BUCKET,
                    blob=blob_name,
                    path=raw_path,
                    method="GET",
                    client_ip=self.client_address[0],
                    zone=ZONE_NAME,
                )
                self.send_with_headers(404, b"not found\n", "text/plain; charset=utf-8")
                return

            data = blob.download_as_bytes()
            ctype, _ = mimetypes.guess_type(blob_name)
            if not ctype:
                ctype = "application/octet-stream"

            self.info(
                "Served file",
                bucket=BUCKET,
                blob=blob_name,
                bytes=len(data),
                client_ip=self.client_address[0],
                zone=ZONE_NAME,
            )
            self.send_with_headers(200, data, ctype)
        except Exception as e:
            log.exception("Server error")
            self.send_with_headers(
                500,
                f"server error: {e}\n".encode("utf-8"),
                "text/plain; charset=utf-8",
            )

    def not_implemented(self):
        self.warn(
            "Method not implemented",
            method=self.command,
            path=self.path,
            client_ip=self.client_address[0],
            zone=ZONE_NAME,
        )
        self.send_with_headers(501, b"not implemented\n", "text/plain; charset=utf-8")

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
    log.info("Listening", extra={"json_fields": {"port": PORT, "zone": ZONE_NAME}})
    server.serve_forever()


if __name__ == "__main__":
    main()