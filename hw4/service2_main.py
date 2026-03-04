import json
import os
from datetime import datetime, timezone

from google.cloud import pubsub_v1
from google.cloud import storage


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_gcs_line(storage_client: storage.Client, bucket_name: str, object_name: str, line: str) -> None:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if blob.exists():
        prev = blob.download_as_text(encoding="utf-8")
        new = prev + line
    else:
        new = line

    blob.upload_from_string(new, content_type="application/json; charset=utf-8")


def main() -> None:
    project_id = os.environ["PROJECT_ID"]
    subscription_id = os.environ["SUBSCRIPTION_ID"]
    bucket_name = os.environ["BUCKET"]
    log_prefix = os.environ.get("LOG_PREFIX", "service2-logs").strip("/")

    sub = pubsub_v1.SubscriberClient()
    sub_path = sub.subscription_path(project_id, subscription_id)

    storage_client = storage.Client(project=project_id)

    log_object = f"{log_prefix}/forbidden_requests.log"

    print(f"[SERVICE2] listening on {sub_path}")

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            payload = json.loads(message.data.decode("utf-8"))
        except Exception:
            payload = {"raw": message.data.decode("utf-8", errors="replace")}

        payload.setdefault("service", "service2")
        payload.setdefault("logged_at", utc_now_iso())

        country = payload.get("country", "?")
        file = payload.get("file", payload.get("object", "?"))
        path = payload.get("path", "?")
        remote = payload.get("remote_addr", "?")
        event_ts = payload.get("event_ts", "?")

        print(
            f"[SERVICE2] forbidden request blocked: "
            f"country={country} file={file} path={path} remote={remote} event_ts={event_ts}"
        )

        append_gcs_line(storage_client, bucket_name, log_object, json.dumps(payload) + "\n")
        message.ack()

    streaming = sub.subscribe(sub_path, callback=callback)
    try:
        streaming.result()
    finally:
        streaming.cancel()


if __name__ == "__main__":
    main()
