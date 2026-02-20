import os
import sys
import json
import time
from datetime import datetime, timezone

import google.auth
from google.auth import impersonated_credentials
from google.cloud import pubsub_v1
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID", "primal-ivy-485619-r6")
TARGET_SA = os.environ.get(
    "TARGET_SA",
    "hw3-svc@primal-ivy-485619-r6.iam.gserviceaccount.com",
)

SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "forbidden-requests-sub")

BUCKET = os.environ.get("BUCKET", "san-hw2-cc")

LOG_PREFIX = os.environ.get("LOG_PREFIX", "service2-logs")
LOG_OBJECT = os.environ.get("LOG_OBJECT", "forbidden_requests.log")



def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_impersonated_creds():
    
    source_creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
   
    return impersonated_credentials.Credentials(
        source_credentials=source_creds,
        target_principal=TARGET_SA,
        target_scopes=[
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/devstorage.read_write",
        ],
        lifetime=3600,
    )


def append_to_gcs(storage_client: storage.Client, line: str):
    bucket = storage_client.bucket(BUCKET)
    obj_name = f"{LOG_PREFIX}/{LOG_OBJECT}".lstrip("/")
    blob = bucket.blob(obj_name)

    
    existing = b""
    try:
        if blob.exists(storage_client):
            existing = blob.download_as_bytes()
    except Exception:
        existing = b""

    new_content = existing + line.encode("utf-8")
    blob.upload_from_string(new_content, content_type="text/plain; charset=utf-8")


def main():
    creds = get_impersonated_creds()

    subscriber = pubsub_v1.SubscriberClient(credentials=creds)
    storage_client = storage.Client(project=PROJECT_ID, credentials=creds)

    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            data = message.data.decode("utf-8", errors="replace")
            payload = json.loads(data)

            country = payload.get("country", "unknown")
            file_ = payload.get("file", "unknown")
            path = payload.get("path", "")
            remote = payload.get("remote_addr", "")
            ts = payload.get("ts", utc_now())

            out = (
                f"[SERVICE2] forbidden request blocked: country={country} "
                f"file={file_} path={path} remote={remote} event_ts={ts}\n"
            )

            sys.stdout.write(out)
            sys.stdout.flush()

            log_line = json.dumps(
                {
                    "service": "service2",
                    "event_type": "forbidden_country",
                    "country": country,
                    "file": file_,
                    "path": path,
                    "remote_addr": remote,
                    "event_ts": ts,
                    "logged_at": utc_now(),
                }
            ) + "\n"
            append_to_gcs(storage_client, log_line)

            message.ack()
        except Exception as e:
            sys.stderr.write(f"[SERVICE2] error: {e}\n")
            sys.stderr.flush()
            message.nack()

    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    sys.stdout.write(f"[SERVICE2] listening on {sub_path} as {TARGET_SA}\n")
    sys.stdout.flush()

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        sys.stdout.write("[SERVICE2] stopped\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()