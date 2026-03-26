#!/usr/bin/env python3
import os
import sys
import time

import pymysql


DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "hw5db")
DB_USER = os.environ.get("DB_USER", "hw5user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "hw5pass123")


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def wait_for_db(max_attempts: int = 20, delay_seconds: int = 3) -> None:
    last_error = None
    for _ in range(max_attempts):
        try:
            conn = get_connection()
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(delay_seconds)
    raise RuntimeError(f"Database never became ready: {last_error}")


def create_tables() -> None:
    conn = get_connection()
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

            
            required_columns = {
                "header_extract_ms": "DOUBLE",
                "storage_read_ms": "DOUBLE",
                "response_send_ms": "DOUBLE",
                "db_insert_ms": "DOUBLE",
                "total_request_ms": "DOUBLE",
            }

            cur.execute("SHOW COLUMNS FROM requests_log")
            existing = {row["Field"] for row in cur.fetchall()}

            for column_name, column_type in required_columns.items():
                if column_name not in existing:
                    cur.execute(
                        f"ALTER TABLE requests_log ADD COLUMN {column_name} {column_type}"
                    )
    finally:
        conn.close()


def main() -> int:
    wait_for_db()
    create_tables()
    print("Schema setup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())