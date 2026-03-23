import functions_framework
from google.cloud import sql_v1

PROJECT_ID = "primal-ivy-485619-r6"
INSTANCE_NAME = "hw5-database"
REGION = "us-central1"

@functions_framework.http
def stop_database(request):
    
    client = sql_v1.SqlInstancesServiceClient()
    
    try:
       
        instance = client.get(project=PROJECT_ID, instance=INSTANCE_NAME)
        
        if instance.state == sql_v1.SqlInstanceState.RUNNABLE:
            
            instance.settings.activation_policy = sql_v1.Settings.SqlActivationPolicy.NEVER
            
            
            operation = client.patch(
                project=PROJECT_ID,
                instance=INSTANCE_NAME,
                database_instance=instance
            )
            
            return {
                "status": "stopped",
                "message": f"Database {INSTANCE_NAME} is being stopped"
            }, 200
        else:
            return {
                "status": "already_stopped",
                "message": f"Database {INSTANCE_NAME} is not running",
                "state": str(instance.state)
            }, 200
            
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }, 500
