#!/usr/bin/env python3

import os
import sys
import pymysql
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from google.cloud import storage, pubsub_v1, logging as cloud_logging
import json
from datetime import datetime


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


def get_db_connection():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        return connection
    except Exception as e:
        logger.log_text(f"Database connection error: {str(e)}", severity='ERROR')
        return None

def insert_successful_request(country, client_ip, gender, age, income, is_banned, requested_file):
    start_time = time.perf_counter()
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO successful_requests 
                (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                country,
                client_ip,
                gender,
                age,
                income,
                is_banned,
                datetime.now(),
                requested_file
            ))
        conn.commit()
        conn.close()
        
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"DB insert (success) took {elapsed:.4f} seconds", severity='INFO')
        
    except Exception as e:
        logger.log_text(f"Error inserting successful request: {str(e)}", severity='ERROR')

def insert_failed_request(requested_file, error_code):
    start_time = time.perf_counter()
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO failed_requests 
                (time_of_request, requested_file, error_code)
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (
                datetime.now(),
                requested_file,
                error_code
            ))
        conn.commit()
        conn.close()
        
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"DB insert (failed) took {elapsed:.4f} seconds", severity='INFO')
        
    except Exception as e:
        logger.log_text(f"Error inserting failed request: {str(e)}", severity='ERROR')

class RequestHandler(BaseHTTPRequestHandler):
    
    def log_message(self, format, *args):
        logger.log_text(f"{self.client_address[0]} - {format % args}", severity='INFO')
    
    def extract_headers(self):
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
        except:
            headers['age'] = 0
        
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"Header extraction took {elapsed:.4f} seconds", severity='DEBUG')
        
        return headers
    
    def fetch_file_from_storage(self, filename):
        start_time = time.perf_counter()
        
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f'html-pages/{filename}')
            content = blob.download_as_bytes()
            
            elapsed = time.perf_counter() - start_time
            logger.log_text(f"Storage read took {elapsed:.4f} seconds", severity='DEBUG')
            
            return content
        except Exception as e:
            logger.log_text(f"Storage read error: {str(e)}", severity='ERROR')
            return None
    
    def send_response_to_client(self, code, content=None, content_type='text/html'):
        start_time = time.perf_counter()
        
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        if content:
            self.send_header('Content-Length', len(content))
        self.end_headers()
        
        if content:
            self.wfile.write(content)
        
        elapsed = time.perf_counter() - start_time
        logger.log_text(f"Response send took {elapsed:.4f} seconds", severity='DEBUG')
    
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
                'timestamp': datetime.now().isoformat()
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
        
        is_banned = headers['country'] in FORBIDDEN_COUNTRIES
        insert_successful_request(
            headers['country'],
            headers['client_ip'],
            headers['gender'],
            headers['age'],
            headers['income'],
            is_banned,
            filename
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
    server = HTTPServer(('0.0.0.0', port), RequestHandler)
    
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
