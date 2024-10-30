import requests
import json

class VaultLoginProcessor:
    def __init__(self, broker_url, vault_url, action_token, provision_role_id=None, wrap_token=False):
        self.broker_url = broker_url
        self.vault_url = vault_url
        self.action_token = action_token
        self.provision_role_id = provision_role_id
        self.wrap_token = wrap_token

    def login(self):

        print("===> Vault login")

        # Get wrapped Vault token
        wrapped_vault_token_json = self._get_wrapped_token()
        print(json.dumps(wrapped_vault_token_json))

        wrapped_vault_token = wrapped_vault_token_json.get('wrap_info', {}).get('token')
        print(f"wrpped token:{wrapped_vault_token}")

        if self.wrap_token:
            vault_token = wrapped_vault_token
        else:
            # Unwrap token
            vault_token = self._unwrap_token(wrapped_vault_token)

        print("Success")
        return vault_token

    def _get_wrapped_token(self):
        """
        Send a request to get a wrapped Vault token.
        """
        headers = {
            'X-Broker-Token': self.action_token
        }

        if self.provision_role_id:
            headers['X-Vault-Role-Id'] = self.provision_role_id

        response = requests.post(f"{self.broker_url}/v1/provision/token/self", headers=headers)

        if response.status_code >= 300:
            error_message = f"Error detected: {response.text}"
            print(error_message)
            raise Exception(error_message)

        return response.json()

    def _unwrap_token(self, wrapped_vault_token):
        """
        Unwrap the wrapped Vault token.
        """
        headers = {
            'X-Vault-Token': wrapped_vault_token
        }

        response = requests.post(f"{self.vault_url}/v1/sys/wrapping/unwrap", headers=headers)
        vault_token_json = response.json()

        if self._has_error(vault_token_json):
            raise Exception("Error detected during token unwrapping")

        vault_token = vault_token_json.get('auth', {}).get('client_token')
        #print(f"vault token:{vault_token}")
        return vault_token

    def _has_error(self, response_json):
        """
        Check if the response contains an error.
        """
        return response_json.get('error') is not None
