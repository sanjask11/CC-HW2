#!/usr/bin/env python3
import os
import sys
import time

from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PROJECT_ID", "")
SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "")

def main():
    if not PROJECT_ID or not SUBSCRIPTION:
        print("Missing PROJECT_ID or SUBSCRIPTION", file=sys.stderr)
        sys.exit(1)

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            print(message.data.decode("utf-8", errors="replace"), flush=True)
        finally:
            message.ack()

    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    print(f"Listening on {sub_path}", flush=True)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
    except Exception as e:
        print(f"Subscriber error: {e}", file=sys.stderr)
        streaming_pull_future.cancel()
        time.sleep(1)
        sys.exit(2)

if __name__ == "__main__":
    main()
