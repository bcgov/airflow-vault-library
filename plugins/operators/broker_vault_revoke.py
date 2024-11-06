import os
import requests

class VaultTokenRevoker:
    def __init__(self, vault_url: str, vault_token: str):
        self.vault_url = vault_url
        self.vault_token = vault_token

    def revoke_token(self):
        print("===> Revoke token")

        try:
            headers = {
                'Content-Type': 'application/json',
                'X-Vault-Token': self.vault_token
            }
            response = requests.post(
                f"{self.vault_url}/v1/auth/token/revoke-self",
                headers=headers
            )
            response.raise_for_status()
            print("Success")
        except requests.exceptions.RequestException as e:
            print(f"Exit: Error detected ({e})")
            print(response.text if response else "No response")
