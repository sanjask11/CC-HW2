import os
from googleapiclient.discovery import build
from google.auth import default

PROJECT_ID = os.environ.get("PROJECT_ID")
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "hw5-db")

def stop_db_if_running(request):
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build("sqladmin", "v1beta4", credentials=credentials, cache_discovery=False)

    instance = service.instances().get(
        project=PROJECT_ID,
        instance=INSTANCE_NAME
    ).execute()

    current_policy = instance["settings"].get("activationPolicy", "ALWAYS")

    if current_policy == "NEVER":
        return {"status": "already_stopped", "instance": INSTANCE_NAME}, 200

    body = {
        "settings": {
            "activationPolicy": "NEVER"
        }
    }

    service.instances().patch(
        project=PROJECT_ID,
        instance=INSTANCE_NAME,
        body=body
    ).execute()

    return {"status": "stopped", "instance": INSTANCE_NAME}, 200
