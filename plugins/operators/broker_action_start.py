import requests

class BrokerActionStart:
    def __init__(self, broker_url, action_token):
        self.broker_url = broker_url
        self.action_token = action_token

    def start_action(self):
        """
        Start the action using the provided action token.
        """
        print("===> Action start")
        headers = {
            'Content-Type': 'application/json',
            'X-Broker-Token': self.action_token
        }

        response = requests.post(f"{self.broker_url}/v1/intention/action/start", headers=headers)

        if response.status_code >= 300:
            raise Exception(f"Error detected ({response.status_code}): {response.text}")

        print("Start action Successfully")
        return response.status_code