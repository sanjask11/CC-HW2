import functions_framework
import google.auth
from googleapiclient import discovery

PROJECT_ID = "primal-ivy-485619-r6"
INSTANCE_NAME = "hw5-database"


@functions_framework.http
def stop_database(request):
    try:
        credentials, _ = google.auth.default()
        service = discovery.build(
            'sqladmin', 'v1beta4',
            credentials=credentials,
            cache_discovery=False,
        )

        instance = service.instances().get(
            project=PROJECT_ID,
            instance=INSTANCE_NAME,
        ).execute()

        state = instance.get('state', '')

        if state == 'RUNNABLE':
            patch_body = {'settings': {'activationPolicy': 'NEVER'}}
            service.instances().patch(
                project=PROJECT_ID,
                instance=INSTANCE_NAME,
                body=patch_body,
            ).execute()
            return {"status": "stopped", "message": f"Database {INSTANCE_NAME} is being stopped"}, 200
        else:
            return {"status": "already_stopped", "message": f"Database {INSTANCE_NAME} is not running", "state": state}, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
