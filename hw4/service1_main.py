#!/usr/bin/env python3
import json
import logging
import mimetypes
import os
import re
import time
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import unquote, urlparse

from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud import storage
from http.server import HTTPServer


PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET = os.environ.get("BUCKET", "")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PORT = int(os.environ.get("PORT", "8080"))
TOPIC = os.environ.get("TOPIC", "")


BANNED = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria",
    "Korea, Democratic People's Republic of", "Democratic People's Republic of Korea",
    "Korea (Democratic People's Republic of)", "DPRK",
}


cloud_logging.Client().setup_logging()
log = logging.getLogger("hw4.service1")
log.setLevel(logging.INFO)


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient() if (PROJECT_ID and TOPIC) else None
topic_path = publisher.topic_path(PROJECT_ID, TOPIC) if publisher else None

SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")


def _client_country(headers) -> str:
    for k in ("X-Country", "X-Client-Country", "X-Geo-Country", "CF-IPCountry"):
        v = headers.get(k)
        if v:
            return v.strip()
    return "Unknown"


def _publish_forbidden(payload: dict) -> None:
    if not publisher or not topic_path:
        return
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    publisher.publish(topic_path, data=data)


def _blob_name_for_path(path: str) -> str:
    
    if path == "" or path == "/":
        path = "index.html"
    
    path = path.lstrip("/")
    return f"{PAGES_PREFIX}/{path}" if PAGES_PREFIX else path


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    server_version = "hw4-service1/1.0"

    def _send(self, code: int, body: bytes, content_type: str = "text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _warn(self, msg: str, **fields):
        log.warning(msg, extra={"json_fields": fields})

    def _critical(self, msg: str, **fields):
        log.critical(msg, extra={"json_fields": fields})

    def _info(self, msg: str, **fields):
        log.info(msg, extra={"json_fields": fields})

    def do_GET(self):
        parsed = urlparse(self.path)
        raw_path = unquote(parsed.path)

        
        if ".." in raw_path or not SAFE_PATH_RE.match(raw_path.lstrip("/")):
            self._warn(
                "Invalid path",
                path=raw_path,
                client_ip=self.client_address[0],
            )
            self._send(404, b"not found\n")
            return

        country = _client_country(self.headers)
        client_ip = self.client_address[0]

        
        if country in BANNED:
            payload = {
                "ts": int(time.time()),
                "country": country,
                "client_ip": client_ip,
                "path": raw_path,
                "method": "GET",
            }
            self._critical("Forbidden country request", **payload)
            _publish_forbidden(payload)
            self._send(400, b"permission denied\n")
            return

        blob_name = _blob_name_for_path(raw_path)

        try:
            bucket = storage_client.bucket(BUCKET)
            blob = bucket.blob(blob_name)
            if not blob.exists():
                self._warn(
                    "File not found",
                    bucket=BUCKET,
                    blob=blob_name,
                    path=raw_path,
                    method="GET",
                    country=country,
                    client_ip=client_ip,
                )
                self._send(404, b"not found\n")
                return

            data = blob.download_as_bytes()
            ctype, _ = mimetypes.guess_type(blob_name)
            if not ctype:
                ctype = "application/octet-stream"
            self._info(
                "Served file",
                bucket=BUCKET,
                blob=blob_name,
                bytes=len(data),
                country=country,
                client_ip=client_ip,
            )
            self._send(200, data, content_type=ctype)
        except Exception as e:
            log.exception("Server error")
            self._send(500, f"server error: {e}\n".encode("utf-8"))

    
    def _not_implemented(self):
        country = _client_country(self.headers)
        self._warn(
            "Method not implemented",
            method=self.command,
            path=self.path,
            country=country,
            client_ip=self.client_address[0],
        )
        self._send(501, b"not implemented\n")

    def do_POST(self): self._not_implemented()
    def do_PUT(self): self._not_implemented()
    def do_DELETE(self): self._not_implemented()
    def do_HEAD(self): self._not_implemented()
    def do_CONNECT(self): self._not_implemented()
    def do_OPTIONS(self): self._not_implemented()
    def do_TRACE(self): self._not_implemented()
    def do_PATCH(self): self._not_implemented()

    def log_message(self, fmt, *args):
        return


def main():
    if not PROJECT_ID or not BUCKET or not TOPIC:
        log.warning(
            "Missing config",
            extra={"json_fields": {"PROJECT_ID": PROJECT_ID, "BUCKET": BUCKET, "TOPIC": TOPIC}},
        )
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("Listening", extra={"json_fields": {"port": PORT}})
    server.serve_forever()


if __name__ == "__main__":
    main()
