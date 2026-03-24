import os

from googleapiclient.discovery import build

PROJECT_ID = os.environ.get("PROJECT_ID", "")
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "hw5-db")

def stop_db(request):
    if not PROJECT_ID:
        return ("Missing PROJECT_ID", 500)

    service = build("sqladmin", "v1beta4", cache_discovery=False)
    instance = service.instances().get(project=PROJECT_ID, instance=INSTANCE_NAME).execute()
    state = instance.get("state", "")

    if state == "RUNNABLE":
        service.instances().patch(
            project=PROJECT_ID,
            instance=INSTANCE_NAME,
            body={"settings": {"activationPolicy": "NEVER"}},
        ).execute()
        return (f"Requested stop for {INSTANCE_NAME}", 200)

    return (f"No action needed. State={state}", 200)