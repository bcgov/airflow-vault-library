import requests
import json

class BrokerOpenIntention:
    def __init__(self,  broker_url, broker_jwt, intention_data, quickstart=False, ttl=None):
        self.broker_url = broker_url
        self.broker_jwt = broker_jwt
        self.quickstart = quickstart
        self.ttl = ttl
        self.intention_data = intention_data

    def open_intention(self):
        """
        Process the intention by sending a request to the broker URL with the given parameters.
        """
        print("===> Intention open")
        json_data = json.dumps(self.intention_data, indent=2)
        print(json_data)  # Print intention data

        # Initialize URL parameters
        url_params = self._build_url_params()
        print(f"broker_jwt {self.broker_jwt}")

        post_url = f"{self.broker_url}/v1/intention/open{url_params}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.broker_jwt}'
        }

        response = requests.post(post_url, headers=headers, json=self.intention_data)

        print(response.status_code)

        if response.status_code >= 300:
            error_message = f"Error detected: {response.text}"
            print(error_message)
            raise Exception(error_message)

        response_data = response.json()
        print(json.dumps(response_data, indent=2))

        # Save intention token and intention ID to the environment
        intention_token = response_data.get('token')
        intention_id = response_data.get('id')
        print(f"intention token: {intention_token}")
        print(f"intention id: {intention_id}")
        action_tokens = self._save_action_tokens(response_data)
        print(f"intention tokens-array: {json.dumps(action_tokens)}")

        print("Success")
        return {
            'intention_token': intention_token,
            'intention_id': intention_id,
            'action_tokens': action_tokens
        }

    def _build_url_params(self):
        """
        Builds URL parameters based on the quickstart flag and TTL.
        """
        url_params = ""
        if self.quickstart:
            url_params += "?quickstart=true"

        if self.ttl is not None and isinstance(self.ttl, int):
            if self.quickstart:
                url_params += f"&ttl={self.ttl}"
            else:
                url_params = f"?ttl={self.ttl}"

        return url_params

    def _save_action_tokens(self, response_data):
        """Extracts action tokens from the response."""
        action_tokens = {}
        print("===> Save action tokens to env")
        action_ids = list(response_data['actions'].keys())

        for action_id in action_ids:
            token = response_data['actions'][action_id]['token']
            trace = response_data['actions'][action_id]['trace_id']
            id_env = f"ACTION_TOKEN_{action_id.upper()}"
            if token:
                action_tokens[id_env] = token
        return action_tokens
