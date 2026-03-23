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

import pymysql
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud import storage


PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET = os.environ.get("BUCKET", "")
PAGES_PREFIX = os.environ.get("PAGES_PREFIX", "html-pages").strip("/")
PORT = int(os.environ.get("PORT", "8080"))
TOPIC = os.environ.get("TOPIC", "")

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "requestsdb")

BANNED = {
    "North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria",
    "Korea, Democratic People's Republic of", "Democratic People's Republic of Korea",
    "Korea (Democratic People's Republic of)", "DPRK",
}

SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")

cloud_logging.Client().setup_logging()
log = logging.getLogger("hw5.service1")
log.setLevel(logging.INFO)

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient() if (PROJECT_ID and TOPIC) else None
topic_path = publisher.topic_path(PROJECT_ID, TOPIC) if publisher else None


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


def extract_header_value(headers, candidates, default=None):
    for key in candidates:
        value = headers.get(key)
        if value is not None and value != "":
            return value.strip()
    return default


def parse_request_metadata(handler):
    start = time.perf_counter_ns()

    headers = handler.headers
    parsed = urlparse(handler.path)
    raw_path = unquote(parsed.path)

    country = extract_header_value(
        headers,
        ["X-Country", "X-Client-Country", "X-Geo-Country", "CF-IPCountry"],
        "Unknown",
    )

    client_ip = extract_header_value(
        headers,
        ["X-Client-IP", "X-Forwarded-For", "X-Real-IP"],
        handler.client_address[0],
    )
    if client_ip and "," in client_ip:
        client_ip = client_ip.split(",")[0].strip()

    gender = extract_header_value(
        headers,
        ["X-Gender", "Gender", "X-User-Gender"],
        "Unknown",
    )

    age_raw = extract_header_value(
        headers,
        ["X-Age", "Age", "X-User-Age"],
        None,
    )

    income_raw = extract_header_value(
        headers,
        ["X-Income", "Income", "X-User-Income"],
        None,
    )

    time_of_day = extract_header_value(
        headers,
        ["X-Time-Of-Day", "Time-Of-Day", "X-User-Time-Of-Day"],
        "Unknown",
    )

    requested_file = "index.html" if raw_path in ("", "/") else raw_path.lstrip("/")

    try:
        age = int(age_raw) if age_raw is not None else None
    except ValueError:
        age = None

    try:
        income = float(income_raw) if income_raw is not None else None
    except ValueError:
        income = None

    is_banned = country in BANNED

    elapsed_ns = time.perf_counter_ns() - start
    return {
        "country": country,
        "client_ip": client_ip,
        "gender": gender,
        "age": age,
        "income": income,
        "time_of_day": time_of_day,
        "requested_file": requested_file,
        "raw_path": raw_path,
        "method": handler.command,
        "is_banned": is_banned,
        "header_extract_ns": elapsed_ns,
    }


def blob_name_for_path(path):
    if path in ("", "/"):
        path = "index.html"
    path = path.lstrip("/")
    return f"{PAGES_PREFIX}/{path}" if PAGES_PREFIX else path


def publish_forbidden(payload):
    if not publisher or not topic_path:
        return
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    publisher.publish(topic_path, data=data)


def insert_request_row(meta, response_code):
    start = time.perf_counter_ns()
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO requests
                (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file, http_method, response_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    meta["country"],
                    meta["client_ip"],
                    meta["gender"],
                    meta["age"],
                    meta["income"],
                    meta["is_banned"],
                    meta["time_of_day"],
                    meta["requested_file"],
                    meta["method"],
                    response_code,
                ),
            )
    finally:
        if conn:
            conn.close()
    return time.perf_counter_ns() - start


def insert_failed_request_row(requested_file, response_code):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO failed_requests
                (requested_file, error_code)
                VALUES (%s, %s)
                """,
                (requested_file, response_code),
            )
    finally:
        if conn:
            conn.close()


def read_from_gcs(requested_file):
    start = time.perf_counter_ns()
    blob_name = blob_name_for_path(requested_file)
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        elapsed_ns = time.perf_counter_ns() - start
        return None, None, elapsed_ns

    data = blob.download_as_bytes()
    ctype, _ = mimetypes.guess_type(blob_name)
    if not ctype:
        ctype = "application/octet-stream"

    elapsed_ns = time.perf_counter_ns() - start
    return data, ctype, elapsed_ns


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    server_version = "hw5-service1/1.0"

    def log_message(self, fmt, *args):
        return

    def send_body(self, code, body, content_type="text/plain; charset=utf-8"):
        start = time.perf_counter_ns()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        return time.perf_counter_ns() - start

    def process_non_get(self):
        meta = parse_request_metadata(self)
        response_code = 501

        send_ns = self.send_body(501, b"not implemented\n")
        db_ns = insert_request_row(meta, response_code)
        insert_failed_request_row(meta["requested_file"], response_code)

        log.warning(
            "Method not implemented",
            extra={"json_fields": {
                "method": self.command,
                "path": self.path,
                "country": meta["country"],
                "client_ip": meta["client_ip"],
                "timing_header_extract_ns": meta["header_extract_ns"],
                "timing_send_response_ns": send_ns,
                "timing_db_insert_ns": db_ns,
            }},
        )

    def do_GET(self):
        meta = parse_request_metadata(self)

        if ".." in meta["raw_path"] or not SAFE_PATH_RE.match(meta["raw_path"].lstrip("/")):
            response_code = 404
            send_ns = self.send_body(404, b"not found\n")
            db_ns = insert_request_row(meta, response_code)
            insert_failed_request_row(meta["requested_file"], response_code)

            log.warning(
                "Invalid path",
                extra={"json_fields": {
                    "path": meta["raw_path"],
                    "client_ip": meta["client_ip"],
                    "timing_header_extract_ns": meta["header_extract_ns"],
                    "timing_send_response_ns": send_ns,
                    "timing_db_insert_ns": db_ns,
                }},
            )
            return

        if meta["is_banned"]:
            payload = {
                "ts": int(time.time()),
                "country": meta["country"],
                "client_ip": meta["client_ip"],
                "path": meta["raw_path"],
                "method": "GET",
            }
            publish_forbidden(payload)

            response_code = 400
            send_ns = self.send_body(400, b"permission denied\n")
            db_ns = insert_request_row(meta, response_code)
            insert_failed_request_row(meta["requested_file"], response_code)

            log.critical(
                "Forbidden country request",
                extra={"json_fields": {
                    **payload,
                    "timing_header_extract_ns": meta["header_extract_ns"],
                    "timing_gcs_read_ns": 0,
                    "timing_send_response_ns": send_ns,
                    "timing_db_insert_ns": db_ns,
                }},
            )
            return

        try:
            data, content_type, gcs_ns = read_from_gcs(meta["requested_file"])

            if data is None:
                response_code = 404
                send_ns = self.send_body(404, b"not found\n")
                db_ns = insert_request_row(meta, response_code)
                insert_failed_request_row(meta["requested_file"], response_code)

                log.warning(
                    "File not found",
                    extra={"json_fields": {
                        "bucket": BUCKET,
                        "requested_file": meta["requested_file"],
                        "country": meta["country"],
                        "client_ip": meta["client_ip"],
                        "timing_header_extract_ns": meta["header_extract_ns"],
                        "timing_gcs_read_ns": gcs_ns,
                        "timing_send_response_ns": send_ns,
                        "timing_db_insert_ns": db_ns,
                    }},
                )
                return

            response_code = 200
            send_ns = self.send_body(200, data, content_type)
            db_ns = insert_request_row(meta, response_code)

            log.info(
                "Served file",
                extra={"json_fields": {
                    "bucket": BUCKET,
                    "requested_file": meta["requested_file"],
                    "bytes": len(data),
                    "country": meta["country"],
                    "client_ip": meta["client_ip"],
                    "timing_header_extract_ns": meta["header_extract_ns"],
                    "timing_gcs_read_ns": gcs_ns,
                    "timing_send_response_ns": send_ns,
                    "timing_db_insert_ns": db_ns,
                }},
            )

        except Exception as e:
            response_code = 500
            send_ns = self.send_body(500, f"server error: {e}\n".encode("utf-8"))
            db_ns = insert_request_row(meta, response_code)
            insert_failed_request_row(meta["requested_file"], response_code)

            log.exception(
                "Server error",
                extra={"json_fields": {
                    "requested_file": meta["requested_file"],
                    "country": meta["country"],
                    "client_ip": meta["client_ip"],
                    "timing_header_extract_ns": meta["header_extract_ns"],
                    "timing_send_response_ns": send_ns,
                    "timing_db_insert_ns": db_ns,
                }},
            )

    def do_POST(self):
        self.process_non_get()

    def do_PUT(self):
        self.process_non_get()

    def do_DELETE(self):
        self.process_non_get()

    def do_HEAD(self):
        self.process_non_get()

    def do_CONNECT(self):
        self.process_non_get()

    def do_OPTIONS(self):
        self.process_non_get()

    def do_TRACE(self):
        self.process_non_get()

    def do_PATCH(self):
        self.process_non_get()


def main():
    missing = [k for k, v in {
        "PROJECT_ID": PROJECT_ID,
        "BUCKET": BUCKET,
        "TOPIC": TOPIC,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_NAME": DB_NAME,
    }.items() if not v]

    if missing:
        log.warning("Missing config", extra={"json_fields": {"missing": missing}})

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("Listening", extra={"json_fields": {"port": PORT}})
    server.serve_forever()


if __name__ == "__main__":
    main()
