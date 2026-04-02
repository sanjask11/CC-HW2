#!/usr/bin/env python3
import ipaddress
import os
import sys
from io import StringIO

import pandas as pd
import pymysql
from google.cloud import storage
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "hw6db")
DB_USER = os.environ.get("DB_USER", "hw6user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "hw6pass123")
PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET_NAME = os.environ.get("BUCKET", "")

COUNTRY_OUTPUT = "hw6/country_predictions.txt"
INCOME_OUTPUT = "hw6/income_predictions.txt"


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
    )


def load_data() -> pd.DataFrame:
    query = """
        SELECT
            r.request_id,
            r.request_time,
            r.client_ip,
            l.country,
            p.gender,
            p.age,
            p.income,
            r.is_banned,
            r.time_of_day,
            r.requested_file,
            r.method,
            r.status_code,
            r.header_extract_ms,
            r.storage_read_ms,
            r.response_send_ms,
            r.db_insert_ms,
            r.total_request_ms
        FROM requests_log_3nf r
        JOIN ip_locations l
          ON r.client_ip = l.client_ip
        LEFT JOIN user_profiles p
          ON r.profile_id = p.profile_id
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


def ip_to_int(ip_value: str) -> int:
    try:
        return int(ipaddress.ip_address(str(ip_value)))
    except Exception:
        return 0


def upload_text_to_gcs(local_text: str, blob_name: str) -> None:
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET environment variable is missing")

    client = storage.Client(project=PROJECT_ID or None)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(local_text, content_type="text/plain")


def build_country_model(df: pd.DataFrame):
    model_df = df[["client_ip", "country"]].dropna().copy()

    if model_df.empty:
        raise RuntimeError("No usable rows for country model")

    model_df["ip_numeric"] = model_df["client_ip"].apply(ip_to_int)

    X = model_df[["ip_numeric"]]
    y = model_df["country"]

    try:
        X_train, X_test, y_train, y_test, raw_train, raw_test = train_test_split(
            X, y, model_df[["client_ip"]], test_size=0.2, random_state=42, stratify=y
        )
    except ValueError:
        X_train, X_test, y_train, y_test, raw_train, raw_test = train_test_split(
            X, y, model_df[["client_ip"]], test_size=0.2, random_state=42
        )

    model = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    out_df = pd.DataFrame({
        "client_ip": raw_test["client_ip"].values,
        "actual_country": y_test.values,
        "predicted_country": y_pred,
    }).reset_index(drop=True)

    return acc, out_df


def build_income_model(df: pd.DataFrame):
    features = [
        "country", "gender", "age", "is_banned", "time_of_day",
        "requested_file", "method", "status_code", "header_extract_ms",
        "storage_read_ms", "response_send_ms", "db_insert_ms", "total_request_ms",
    ]

    # Only use features that actually exist and have data
    available = [f for f in features if f in df.columns and df[f].notna().any()]
    model_df = df[available + ["income"]].dropna(subset=["income"]).copy()

    if model_df.empty:
        raise RuntimeError("No usable rows for income model (income column is all NULL)")

    X = model_df[available]
    y = model_df["income"]

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
    except ValueError:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

    categorical_features = [f for f in ["country", "gender", "time_of_day", "requested_file", "method"] if f in available]
    numeric_features = [f for f in available if f not in categorical_features]

    transformers = []
    if categorical_features:
        transformers.append(("cat", Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]), categorical_features))
    if numeric_features:
        transformers.append(("num", Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]), numeric_features))

    model = Pipeline(steps=[
        ("preprocessor", ColumnTransformer(transformers=transformers)),
        ("classifier", RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1)),
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    out_df = X_test.copy().reset_index(drop=True)
    out_df["actual_income"] = y_test.reset_index(drop=True)
    out_df["predicted_income"] = y_pred

    return acc, out_df


def dataframe_to_text(title: str, accuracy: float, df: pd.DataFrame, max_rows: int = 200) -> str:
    buf = StringIO()
    buf.write(f"{title}\n")
    buf.write(f"accuracy={accuracy:.4f}\n")
    buf.write(f"rows_shown={min(len(df), max_rows)} of {len(df)}\n\n")
    buf.write(df.head(max_rows).to_string(index=False))
    buf.write("\n")
    return buf.getvalue()

def main():
    print("Loading normalized data from Cloud SQL...")
    df = load_data()
    print(f"Loaded {len(df)} rows")

    if df.empty:
        raise RuntimeError("No data found in requests_log_3nf join result")

    print("Training country model...")
    country_acc, country_out = build_country_model(df)
    country_text = dataframe_to_text("Country Prediction Results", country_acc, country_out)
    upload_text_to_gcs(country_text, COUNTRY_OUTPUT)
    print(f"Country model accuracy: {country_acc:.4f}")

    print("Training income model...")
    try:
        income_acc, income_out = build_income_model(df)
        income_text = dataframe_to_text("Income Prediction Results", income_acc, income_out)
        upload_text_to_gcs(income_text, INCOME_OUTPUT)
        print(f"Income model accuracy: {income_acc:.4f}")
    except RuntimeError as e:
        print(f"WARNING: Income model skipped — {e}")
        upload_text_to_gcs(f"Income Prediction Results\naccuracy=N/A\n{e}\n", INCOME_OUTPUT)

    print("HW6 model run complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)