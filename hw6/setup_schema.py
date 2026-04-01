#!/usr/bin/env python3
import os
import sys
import time
import pymysql

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "hw6db")
DB_USER = os.environ.get("DB_USER", "hw6user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "hw6pass123")


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4"
    )


def wait_for_db():
    for _ in range(30):
        try:
            c = get_connection()
            c.close()
            return
        except:
            time.sleep(3)
    raise RuntimeError("Database never became ready")


def list_tables(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    key = f"Tables_in_{DB_NAME}"
    return [row[key] for row in rows]


def get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = cur.fetchall()
    return [r["Field"] for r in rows]


def find_source_table(conn):
    for t in list_tables(conn):
        cols = set(get_columns(conn, t))
        if "client_ip" in cols and "country" in cols:
            return t, cols
    raise RuntimeError("Cannot locate raw request table")


def find_error_table(conn, source):
    for t in list_tables(conn):
        if t == source:
            continue
        cols = set(get_columns(conn, t))
        if "requested_file" in cols and ("error_code" in cols or "status_code" in cols):
            return t, cols
    return None, set()


def ensure_tables(conn):
    with conn.cursor() as cur:

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ip_locations(
            client_ip VARCHAR(64) PRIMARY KEY,
            country VARCHAR(100)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles(
            profile_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            gender VARCHAR(20),
            age INT,
            income VARCHAR(50),
            UNIQUE KEY uq_profile(gender,age,income)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS requests_log_3nf(
            request_id BIGINT PRIMARY KEY,
            request_time TIMESTAMP NULL,
            client_ip VARCHAR(64),
            profile_id BIGINT,
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
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS error_requests_3nf(
            error_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            request_time TIMESTAMP NULL,
            requested_file VARCHAR(255),
            error_code INT
        )
        """)


def clear_tables(conn):
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute("TRUNCATE ip_locations")
        cur.execute("TRUNCATE user_profiles")
        cur.execute("TRUNCATE requests_log_3nf")
        cur.execute("TRUNCATE error_requests_3nf")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")


def safe_datetime(col):
    return f"NULLIF({col}, '0000-00-00 00:00:00')"


def migrate(conn, source, cols, err_table, err_cols):

    with conn.cursor() as cur:
        cur.execute("SET SESSION sql_mode = ''")
        cur.execute("SET SESSION time_zone = '+00:00'")hw6db

        cur.execute(f"""
        INSERT INTO ip_locations(client_ip, country)
        SELECT client_ip, MIN(country)
        FROM `{source}`
        GROUP BY client_ip
        """)

        gender_sel = "r.`gender`" if "gender" in cols else "NULL"
        age_sel    = "r.`age`"    if "age"    in cols else "NULL"
        income_sel = "r.`income`" if "income" in cols else "NULL"

        cur.execute(f"""
        INSERT IGNORE INTO user_profiles(gender, age, income)
        SELECT DISTINCT {gender_sel}, {age_sel}, {income_sel}
        FROM `{source}` r
        """)

        request_time   = safe_datetime("r.request_time")  if "request_time"      in cols else "NULL"
        requested_file = "r.requested_file"               if "requested_file"    in cols else "NULL"
        method         = "r.method"                       if "method"            in cols else "NULL"
        status_code    = "r.status_code"                  if "status_code"       in cols else "NULL"
        is_banned      = "r.is_banned"                    if "is_banned"         in cols else "NULL"
        time_of_day    = "r.time_of_day"                  if "time_of_day"       in cols else "NULL"
        header         = "r.header_extract_ms"            if "header_extract_ms" in cols else "NULL"
        storage        = "r.storage_read_ms"              if "storage_read_ms"   in cols else "NULL"
        send           = "r.response_send_ms"             if "response_send_ms"  in cols else "NULL"
        db             = "r.db_insert_ms"                 if "db_insert_ms"      in cols else "NULL"
        total          = "r.total_request_ms"             if "total_request_ms"  in cols else "NULL"

        cur.execute(f"""
        INSERT INTO requests_log_3nf
        SELECT
            ROW_NUMBER() OVER(),
            {request_time},
            r.client_ip,
            p.profile_id,
            {is_banned},
            {time_of_day},
            {requested_file},
            {method},
            {status_code},
            {header},
            {storage},
            {send},
            {db},
            {total}
        FROM `{source}` r
        LEFT JOIN user_profiles p
          ON {gender_sel} <=> p.gender
         AND {age_sel}    <=> p.age
         AND {income_sel} <=> p.income
        """)

        if err_table:
            request_time_err   = safe_datetime("e.request_time") if "request_time"   in err_cols else "NULL"
            requested_file_err = "e.requested_file"              if "requested_file" in err_cols else "NULL"
            err_col            = "e.error_code"                  if "error_code"     in err_cols else "e.status_code"

            cur.execute(f"""
            INSERT INTO error_requests_3nf(request_time, requested_file, error_code)
            SELECT {request_time_err}, {requested_file_err}, {err_col}
            FROM `{err_table}` e
            """)



def main():

    wait_for_db()

    conn = get_connection()

    try:

        source, cols = find_source_table(conn)
        err_table, err_cols = find_error_table(conn, source)

        ensure_tables(conn)
        clear_tables(conn)

        migrate(conn, source, cols, err_table, err_cols)

        print("3NF schema setup complete")

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())