import functions_framework
from google.cloud import sql_v1

PROJECT_ID = "primal-ivy-485619-r6"  # Your project ID
INSTANCE_NAME = "hw5-database"
REGION = "us-central1"

@functions_framework.http
def stop_database(request):
    
    client = sql_v1.SqlInstancesServiceClient()
    instance_path = f"projects/{PROJECT_ID}/instances/{INSTANCE_NAME}"
    
    try:
        instance = client.get(project=PROJECT_ID, instance=INSTANCE_NAME)
        
        if instance.state == sql_v1.SqlInstanceState.RUNNABLE:
            operation = client.patch(
                project=PROJECT_ID,
                instance=INSTANCE_NAME,
                body={
                    "settings": {
                        "activationPolicy": "NEVER"
                    }
                }
            )
            
            return {
                "status": "stopped",
                "message": f"Database {INSTANCE_NAME} is being stopped",
                "instance": INSTANCE_NAME
            }, 200
        else:
            return {
                "status": "already_stopped",
                "message": f"Database {INSTANCE_NAME} is not running",
                "instance": INSTANCE_NAME,
                "state": str(instance.state)
            }, 200
            
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }, 500

