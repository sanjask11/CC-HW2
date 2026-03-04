import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud import storage

FORBIDDEN = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Service1Handler(BaseHTTPRequestHandler):
    server_version = "hw4-service1/1.0"

    
    bucket_name: str = ""
    topic_path: str = ""
    pages_prefix: str = ""
    publisher: pubsub_v1.PublisherClient = None  
    storage_client: storage.Client = None  
    log: cloud_logging.Logger = None  

    def _write(self, code: int, body: str, content_type: str = "text/plain; charset=utf-8") -> None:
        b = body.encode("utf-8", errors="replace")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _structured_log(self, severity: str, payload: dict) -> None:
        self.log.log_struct(payload, severity=severity)

    def _publish_forbidden(self, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.publisher.publish(self.topic_path, data=data)

    def _get_country(self) -> str:
        return (self.headers.get("X-country") or "").strip()

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if not path.startswith("/"):
            path = "/" + path
        obj = path.lstrip("/")

        if self.pages_prefix and not obj.startswith(self.pages_prefix.rstrip("/") + "/"):
            obj = f"{self.pages_prefix.rstrip('/')}/{obj}".lstrip("/")

        country = self._get_country()
        remote = self.client_address[0] if self.client_address else ""
        ts = utc_now_iso()

        if country in FORBIDDEN:
            event = {
                "service": "service1",
                "event_type": "forbidden_country",
                "country": country,
                "file": obj.split("/")[-1],
                "path": path,
                "remote_addr": remote,
                "event_ts": ts,
            }
            self._structured_log("CRITICAL", event)
            self._publish_forbidden(event)
            self._write(400, "Permission denied\n")
            return

        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(obj)

        if not blob.exists():
            event = {
                "service": "service1",
                "event_type": "not_found",
                "object": obj,
                "path": path,
                "remote_addr": remote,
                "event_ts": ts,
            }
            self._structured_log("WARNING", event)
            self._write(404, "Not Found\n")
            return

        data = blob.download_as_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _not_implemented(self) -> None:
        method = self.command
        path = self.path.split("?", 1)[0]
        remote = self.client_address[0] if self.client_address else ""
        ts = utc_now_iso()
        event = {
            "service": "service1",
            "event_type": "not_implemented",
            "method": method,
            "path": path,
            "remote_addr": remote,
            "event_ts": ts,
        }
        self._structured_log("WARNING", event)
        self._write(501, "Not Implemented\n")

    def do_POST(self) -> None: self._not_implemented()
    def do_PUT(self) -> None: self._not_implemented()
    def do_DELETE(self) -> None: self._not_implemented()
    def do_HEAD(self) -> None: self._not_implemented()
    def do_CONNECT(self) -> None: self._not_implemented()
    def do_OPTIONS(self) -> None: self._not_implemented()
    def do_TRACE(self) -> None: self._not_implemented()
    def do_PATCH(self) -> None: self._not_implemented()

    def log_message(self, fmt: str, *args) -> None:
        return


def main() -> None:
    project_id = os.environ["PROJECT_ID"]
    bucket_name = os.environ["BUCKET"]
    pages_prefix = os.environ.get("PAGES_PREFIX", "html-pages")
    topic_id = os.environ["TOPIC"]
    port = int(os.environ.get("PORT", "8080"))

    storage_client = storage.Client(project=project_id)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    logging_client = cloud_logging.Client(project=project_id)
    logger = logging_client.logger("hw4-service1")

    Service1Handler.bucket_name = bucket_name
    Service1Handler.pages_prefix = pages_prefix
    Service1Handler.publisher = publisher
    Service1Handler.topic_path = topic_path
    Service1Handler.storage_client = storage_client
    Service1Handler.log = logger

    server = ThreadingHTTPServer(("0.0.0.0", port), Service1Handler)
    print(f"[SERVICE1] listening on 0.0.0.0:{port} bucket={bucket_name} prefix={pages_prefix} topic={topic_id}")
    server.serve_forever()


if __name__ == "__main__":
    main()
