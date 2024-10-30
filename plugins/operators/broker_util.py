class BrokerUtils:
    def __init__(self, project_name, service_name, target_env):
        self.project_name = project_name
        self.service_name = service_name
        self.target_env = target_env

    def build_json_template(self):
    # Define the JSON template
        json_template = {
                "event": {
                    "provider": "airflow",
                    "reason": "Job triggered",
                    "url": "http://localhost:8080/airflow"
                },
                "actions": [
                    {
                        "action": "server-access",
                        "id": "login",
                        "provision": ["token/self"],
                        "service": {
                            "name": self.service_name,
                            "project": self.project_name,
                            "environment": self.target_env
                        }
                    },
                    {
                        "action": "database-access",
                        "id": "database",
                        "provision": ["token/self"],
                        "service": {
                            "name": self.service_name,
                            "project": self.project_name,
                            "environment": self.target_env
                        }
                    },
                ],
                "user": {
                    "name": "gruan@azureidir"
                }
            }
        return json_template
