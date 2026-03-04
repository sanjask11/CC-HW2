import json
import os
import sys
from datetime import datetime, timezone

from google.cloud import pubsub_v1


SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "forbidden-requests-sub")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _project_id() -> str:
    return os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT", "")


def main():
    pid = _project_id()
    if not pid:
        print("ERROR: Missing GOOGLE_CLOUD_PROJECT / GCLOUD_PROJECT", file=sys.stderr, flush=True)
        sys.exit(2)

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(pid, SUBSCRIPTION)

    print(f"[{_utc_now_iso()}] Service2 listening on {sub_path}", flush=True)

    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            payload = json.loads(message.data.decode("utf-8"))
        except Exception:
            payload = {"raw": message.data.decode("utf-8", errors="replace")}

        country = payload.get("country", "UNKNOWN")
        path = payload.get("path", "UNKNOWN")
        method = payload.get("method", "UNKNOWN")
        ts = payload.get("timestamp", _utc_now_iso())

        print(f"[{ts}] FORBIDDEN REQUEST country={country} method={method} path={path}", flush=True)
        message.ack()

    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    main()
