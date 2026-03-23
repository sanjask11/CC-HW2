#!/usr/bin/env python3

import os
import time
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

from google.cloud import storage, pubsub_v1, logging as cloud_logging
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = int(os.environ.get('DB_PORT', 3306))
DB_NAME = os.environ.get('DB_NAME', 'request_logs')
DB_USER = os.environ.get('DB_USER', 'webserver')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')
PROJECT_ID = os.environ.get('PROJECT_ID', '')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'san-hw2-cc')

FORBIDDEN_COUNTRIES = [
    "North Korea", "Iran", "Cuba", "Syria", "Russia", "China"
]


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, 'forbidden-requests')

logging_client = cloud_logging.Client()
logger = logging_client.logger('hw5-webserver')

# SQLAlchemy connection pool – avoids opening a new DB connection per request.
# pool_size=10 keeps 10 connections open; max_overflow=20 allows 20 extra
# under burst load (30 total), which comfortably handles 2 concurrent clients.
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 10},
)



def insert_successful_request(country, client_ip, gender, age, income, is_banned, requested_file):
    start_time = time.perf_counter()
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO successful_requests
                    (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
                VALUES
                    (:country, :client_ip, :gender, :age, :income, :is_banned, :time_of_day, :requested_file)
            """), {
                'country': country,
                'client_ip': client_ip,
                'gender': gender,
                'age': age,
                'income': income,
                'is_banned': is_banned,
                'time_of_day': datetime.now(),
                'requested_file': requested_file,
            })
            conn.commit()
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"DB insert (success) took {elapsed:.4f}s", severity='INFO')
    except Exception as e:
        logger.log_text(f"Error inserting successful request: {e}", severity='ERROR')


def insert_failed_request(requested_file, error_code):
    start_time = time.perf_counter()
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO failed_requests
                    (time_of_request, requested_file, error_code)
                VALUES
                    (:time_of_request, :requested_file, :error_code)
            """), {
                'time_of_request': datetime.now(),
                'requested_file': requested_file,
                'error_code': error_code,
            })
            conn.commit()
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"DB insert (failed) took {elapsed:.4f}s", severity='INFO')
    except Exception as e:
        logger.log_text(f"Error inserting failed request: {e}", severity='ERROR')




class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a separate thread so 2 concurrent clients
    do not block each other."""
    daemon_threads = True




class RequestHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        logger.log_text(f"{self.client_address[0]} - {format % args}", severity='INFO')

    def extract_headers(self):
        """Extract and parse all custom request headers."""
        start_time = time.perf_counter()
        headers = {
            'country': self.headers.get('X-country', 'Unknown'),
            'client_ip': self.client_address[0],
            'gender': self.headers.get('X-gender', 'Unknown'),
            'age': self.headers.get('X-age', '0'),
            'income': self.headers.get('X-income', 'Unknown'),
        }
        try:
            headers['age'] = int(headers['age'])
        except (ValueError, TypeError):
            headers['age'] = 0
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"Header extraction took {elapsed:.6f}s", severity='DEBUG')
        return headers

    def fetch_file_from_storage(self, filename):
        """Download a file from Cloud Storage."""
        start_time = time.perf_counter()
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f'html-pages/{filename}')
            content = blob.download_as_bytes()
            elapsed = time.perf_counter() - start_time
            logger.log_text(f"Storage read took {elapsed:.4f}s", severity='DEBUG')
            return content
        except Exception as e:
            logger.log_text(f"Storage read error: {e}", severity='ERROR')
            return None

    def send_response_to_client(self, code, content=None, content_type='text/html'):
        """Write the HTTP response to the client."""
        start_time = time.perf_counter()
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        if content:
            self.send_header('Content-Length', len(content))
        self.end_headers()
        if content:
            self.wfile.write(content)
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"Response send took {elapsed:.6f}s", severity='DEBUG')

    def do_GET(self):
        headers = self.extract_headers()
        filename = self.path.strip('/')
        if not filename:
            filename = 'index.html'

        if headers['country'] in FORBIDDEN_COUNTRIES:
            logger.log_text(f"Forbidden request from {headers['country']}", severity='CRITICAL')
            message_data = json.dumps({
                'country': headers['country'],
                'client_ip': headers['client_ip'],
                'file': filename,
                'timestamp': datetime.now().isoformat(),
            }).encode('utf-8')
            publisher.publish(topic_path, message_data)
            self.send_response_to_client(400, b'Permission denied')
            insert_failed_request(filename, 400)
            return

        content = self.fetch_file_from_storage(filename)
        if content is None:
            self.send_response_to_client(404, b'Not found')
            insert_failed_request(filename, 404)
            return

        self.send_response_to_client(200, content)
        insert_successful_request(
            headers['country'], headers['client_ip'], headers['gender'],
            headers['age'], headers['income'], False, filename,
        )

    def do_POST(self):
        self.send_response_to_client(501, b'Not implemented')
        insert_failed_request(self.path.strip('/'), 501)

    def do_PUT(self):
        self.send_response_to_client(501, b'Not implemented')
        insert_failed_request(self.path.strip('/'), 501)

    def do_DELETE(self):
        self.send_response_to_client(501, b'Not implemented')
        insert_failed_request(self.path.strip('/'), 501)


def main():
    port = 8080
    server = ThreadingHTTPServer(('0.0.0.0', port), RequestHandler)
    print(f"Server starting on port {port}...")
    logger.log_text(f"Web server started on port {port}", severity='INFO')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        logger.log_text("Web server shutting down", severity='INFO')
        server.shutdown()


if __name__ == '__main__':
    main()
