#!/usr/bin/env python3
import json
import logging
import mimetypes
import os
import re
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse

import pymysql
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "primal-ivy-485619-r6"
BUCKET = os.environ.get("BUCKET", "")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PORT = int(os.environ.get("PORT", "8080"))
TOPIC = os.environ.get("TOPIC", "")

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "hw5db")
DB_USER = os.environ.get("DB_USER", "hw5user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "hw5pass123")

BANNED = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria",
    "Korea, Democratic People's Republic of", "Democratic People's Republic of Korea",
    "Korea (Democratic People's Republic of)", "DPRK",
}

SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")

cloud_logging.Client(project=PROJECT_ID).setup_logging()
log = logging.getLogger("hw5.service1")
log.setLevel(logging.INFO)

storage_client = storage.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient() if (PROJECT_ID and TOPIC) else None
topic_path = publisher.topic_path(PROJECT_ID, TOPIC) if publisher else None


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_tables() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS requests_log (
                    request_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    country VARCHAR(100),
                    client_ip VARCHAR(64),
                    gender VARCHAR(20),
                    age INT,
                    income VARCHAR(50),
                    is_banned BOOLEAN,
                    time_of_day VARCHAR(20),
                    requested_file VARCHAR(255),
                    method VARCHAR(10),
                    status_code INT,
                    header_extract_ms DOUBLE,
                    storage_read_ms DOUBLE,
                    response_send_ms DOUBLE,
                    db_insert_ms DOUBLE,
                    total_request_ms DOUBLE
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS error_requests (
                    error_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    requested_file VARCHAR(255),
                    error_code INT
                )
                """
            )
    finally:
        conn.close()


def normalize_country(headers) -> str:
    for key in ("X-Country", "X-Client-Country", "X-Geo-Country", "CF-IPCountry"):
        value = headers.get(key)
        if value:
            return value.strip()
    return "Unknown"


def first_header(headers, *keys: str) -> str:
    for key in keys:
        value = headers.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def parse_age(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        digits = "".join(ch for ch in value if ch.isdigit())
        return int(digits) if digits else None


def extract_time_of_day(headers) -> str:
    value = first_header(headers, "X-Time-Of-Day", "Time-Of-Day", "X-Client-Time-Of-Day")
    if value:
        return value
    hour = time.gmtime().tm_hour
    if 5 <= hour < 12:
        return "morning"
    if 12 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 21:
        return "evening"
    return "night"


def blob_name_for_path(raw_path: str) -> str:
    clean = raw_path.lstrip("/")
    if clean == "":
        clean = "index.html"
    return f"{PAGES_PREFIX}/{clean}" if PAGES_PREFIX else clean


def read_blob(bucket_name: str, blob_name: str) -> Tuple[Optional[bytes], Optional[str]]:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None, None
    data = blob.download_as_bytes()
    ctype, _ = mimetypes.guess_type(blob_name)
    if not ctype:
        ctype = "application/octet-stream"
    return data, ctype


def publish_forbidden(payload: Dict[str, Any]) -> None:
    if not publisher or not topic_path:
        return
    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))


def insert_request_row(metadata: Dict[str, Any], timings: Dict[str, float], status_code: int) -> float:
    start = time.perf_counter()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO requests_log (
                    country, client_ip, gender, age, income, is_banned, time_of_day,
                    requested_file, method, status_code,
                    header_extract_ms, storage_read_ms, response_send_ms, db_insert_ms, total_request_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    metadata["country"],
                    metadata["client_ip"],
                    metadata["gender"],
                    metadata["age"],
                    metadata["income"],
                    int(bool(metadata["is_banned"])),
                    metadata["time_of_day"],
                    metadata["requested_file"],
                    metadata["method"],
                    status_code,
                    timings["header_extract_ms"],
                    timings["storage_read_ms"],
                    timings["response_send_ms"],
                    0.0,
                    timings["total_request_ms"],
                ),
            )
            request_id = cur.lastrowid
    finally:
        conn.close()

    db_insert_ms = (time.perf_counter() - start) * 1000.0

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests_log SET db_insert_ms=%s WHERE request_id=%s",
                (db_insert_ms, request_id),
            )
    finally:
        conn.close()

    return db_insert_ms


def insert_error_row(requested_file: str, error_code: int) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO error_requests (requested_file, error_code)
                VALUES (%s, %s)
                """,
                (requested_file, error_code),
            )
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "hw5-service1/1.0"

    def _send(self, code: int, body: bytes, content_type: str = "text/plain; charset=utf-8") -> float:
        start = time.perf_counter()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        return (time.perf_counter() - start) * 1000.0

    def _extract_metadata(self, method: str) -> Tuple[Dict[str, Any], float]:
        start = time.perf_counter()
        raw_path = unquote(urlparse(self.path).path or "/")
        requested_file = raw_path.lstrip("/")
        metadata = {
            "country": normalize_country(self.headers),
            "client_ip": self.client_address[0],
            "gender": first_header(self.headers, "X-Gender", "Gender", "X-Client-Gender"),
            "age": parse_age(first_header(self.headers, "X-Age", "Age", "X-Client-Age")),
            "income": first_header(self.headers, "X-Income", "Income", "X-Client-Income"),
            "time_of_day": extract_time_of_day(self.headers),
            "requested_file": requested_file,
            "method": method,
            "raw_path": raw_path,
        }
        metadata["is_banned"] = metadata["country"] in BANNED
        return metadata, (time.perf_counter() - start) * 1000.0

    def _handle_get(self) -> None:
        total_start = time.perf_counter()
        metadata, header_ms = self._extract_metadata("GET")

        timings = {
            "header_extract_ms": header_ms,
            "storage_read_ms": 0.0,
            "response_send_ms": 0.0,
            "db_insert_ms": 0.0,
            "total_request_ms": 0.0,
        }

        try:
            raw_path = metadata["raw_path"]

            if ".." in raw_path or not SAFE_PATH_RE.match(raw_path.lstrip("/")):
                log.warning("Invalid path", extra={"json_fields": metadata})
                timings["response_send_ms"] = self._send(404, b"not found\n")
                timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
                timings["db_insert_ms"] = insert_request_row(metadata, timings, 404)
                insert_error_row(metadata["requested_file"], 404)
                return

            if metadata["is_banned"]:
                payload = {
                    "country": metadata["country"],
                    "client_ip": metadata["client_ip"],
                    "path": raw_path,
                    "method": "GET",
                    "time": time.time(),
                }
                log.critical("Forbidden country request", extra={"json_fields": payload})
                publish_forbidden(payload)
                timings["response_send_ms"] = self._send(400, b"permission denied\n")
                timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
                timings["db_insert_ms"] = insert_request_row(metadata, timings, 400)
                insert_error_row(metadata["requested_file"], 400)
                return

            blob_name = blob_name_for_path(raw_path)
            read_start = time.perf_counter()
            data, ctype = read_blob(BUCKET, blob_name)
            timings["storage_read_ms"] = (time.perf_counter() - read_start) * 1000.0

            if data is None:
                log.warning("File not found", extra={"json_fields": {**metadata, "blob": blob_name}})
                timings["response_send_ms"] = self._send(404, b"not found\n")
                timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
                timings["db_insert_ms"] = insert_request_row(metadata, timings, 404)
                insert_error_row(metadata["requested_file"], 404)
                return

            log.info("Served file", extra={"json_fields": {**metadata, "blob": blob_name, "bytes": len(data)}})
            timings["response_send_ms"] = self._send(200, data, content_type=ctype)
            timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
            timings["db_insert_ms"] = insert_request_row(metadata, timings, 200)

        except Exception:
            log.exception("Server error")
            try:
                timings["response_send_ms"] = self._send(500, b"server error\n")
            except Exception:
                pass
            timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
            try:
                timings["db_insert_ms"] = insert_request_row(metadata, timings, 500)
                insert_error_row(metadata["requested_file"], 500)
            except Exception:
                log.exception("Failed to record internal error")

    def _handle_not_implemented(self) -> None:
        total_start = time.perf_counter()
        metadata, header_ms = self._extract_metadata(self.command)

        timings = {
            "header_extract_ms": header_ms,
            "storage_read_ms": 0.0,
            "response_send_ms": 0.0,
            "db_insert_ms": 0.0,
            "total_request_ms": 0.0,
        }

        log.warning("Method not implemented", extra={"json_fields": metadata})
        timings["response_send_ms"] = self._send(501, b"not implemented\n")
        timings["total_request_ms"] = (time.perf_counter() - total_start) * 1000.0
        timings["db_insert_ms"] = insert_request_row(metadata, timings, 501)
        insert_error_row(metadata["requested_file"], 501)

    def do_GET(self):
        self._handle_get()

    def do_POST(self):
        self._handle_not_implemented()

    def do_PUT(self):
        self._handle_not_implemented()

    def do_DELETE(self):
        self._handle_not_implemented()

    def do_HEAD(self):
        self._handle_not_implemented()

    def do_CONNECT(self):
        self._handle_not_implemented()

    def do_OPTIONS(self):
        self._handle_not_implemented()

    def do_TRACE(self):
        self._handle_not_implemented()

    def do_PATCH(self):
        self._handle_not_implemented()

    def log_message(self, fmt, *args):
        return


def main():
    ensure_tables()

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