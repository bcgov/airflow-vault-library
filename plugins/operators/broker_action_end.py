
import requests

class BrokerActionEnd:
    def __init__(self, broker_url, action_token):
        self.broker_url = broker_url
        self.action_token = action_token

    def end_action(self):
        """
        End the action using the provided action token.
        """
        print("===> Action end")
        headers = {
            'Content-Type': 'application/json',
            'X-Broker-Token': self.action_token
        }

        response = requests.post(f"{self.broker_url}/v1/intention/action/end", headers=headers)

        if response.status_code >= 300:
            raise Exception(f"Error detected ({response.status_code}): {response.text}")

        print("End action successly")
        return response.status_code
