import requests

class BrokerCloseIntention:
    def __init__(self, broker_url, intention_token):
        self.broker_url = broker_url
        self.intention_token = intention_token

    def close_intention(self):
        """
        Close the intention using the provided intention token.
        """
        print("===> Intention close")
        headers = {
            'X-Broker-Token': self.intention_token
        }

        response = requests.post(f"{self.broker_url}/v1/intention/close", headers=headers)
        if response.status_code >= 300:
            raise Exception(f"Error detected ({response.status_code}): {response.text}")

        print("Close intention successly")
        return response.status_code
