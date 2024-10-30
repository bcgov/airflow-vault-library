import requests
import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.broker_util import BrokerUtils
from operators.broker_intention_open import BrokerOpenIntention
from operators.broker_action_start import BrokerActionStart
from operators.broker_vault_login import VaultLoginProcessor
from operators.broker_action_end import BrokerActionEnd

class VaultSecretRequestOperator(BaseOperator):
    @apply_defaults
    def __init__(self, project_name, service_name, vault_url, broker_url, target_env, broker_jwt, subpath="", role_id=None, wrap_token=False, quickstart=False, ttl=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_name = project_name
        self.service_name = service_name
        self.vault_url = vault_url
        self.broker_url = broker_url
        self.target_env = target_env
        self.broker_jwt = broker_jwt
        self.quickstart = quickstart
        self.ttl = ttl
        self.subpath = subpath
        self.role_id = role_id
        self.wrap_token = wrap_token

    def get_short_env(self):
        if self.target_env == 'production':
            return 'prod'
        elif self.target_env == 'test':
            return 'test'
        elif self.target_env == 'development':
            return 'dev'
        else:
            return 'dev'

    def execute(self, context):
        self.log.info("Starting to fetch secrets from Vault...")
        self.log.info(f"Connecting to Vault at {self.vault_url} with project {self.project_name}")

        print(f"run build json file")
        broker_utils = BrokerUtils(
            project_name=self.project_name,
            service_name=self.service_name,
            target_env=self.target_env
        )
        payload = broker_utils.build_json_template()
        print(f"Open Intention ...")
        intention_response = BrokerOpenIntention(self.broker_url,self.broker_jwt,payload).open_intention()
        action_tokens = intention_response['action_tokens']
        vault_login_token = action_tokens.get('ACTION_TOKEN_LOGIN')
        vault_database_token = action_tokens.get('ACTION_TOKEN_DATABASE')
        print(f"Action starts ...")
        vault_action_start = BrokerActionStart(self.broker_url,vault_login_token).start_action()
        print(f"Action starts with return code {vault_action_start}")

        for action_id, token in action_tokens.items():
            print(f"Action ID: {action_id}, Token: {token}")
        print(f"Vault login ...")
        vault_token = VaultLoginProcessor(
            broker_url=self.broker_url,
            vault_url=self.vault_url,
            action_token=vault_login_token,
            provision_role_id=self.role_id,
            wrap_token=self.wrap_token
        ).login()

        short_env= self.get_short_env()
        print(f"short environment: {short_env}")

        if self.subpath:
            secrets_path = f"{short_env}/{self.project_name}/{self.service_name}/{self.subpath}"
        else:
            secrets_path = f"{short_env}/{self.project_name}/{self.service_name}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {vault_token}'
        }
        response = requests.get(f'{self.vault_url}/v1/apps/data/{secrets_path}', headers=headers)
        if response.status_code >= 300:
            error_message = f"Error detected: {response.text}"
            print(error_message)
            raise Exception(f'Failed to retrieve secrets from Vault. Status code: {response.status_code}, Error: {response.text}')

        # Extract secrets from the response JSON
        secrets_data = response.json().get('data', {}).get('data', {})
        if not secrets_data:
            raise Exception("No secrets found in the provided Vault path.")
        #print(f"secrets data: {secrets_data}")
        secrets = {key: value for key, value in secrets_data.items()}
        #print(f"formatted secrets data: {secrets}")

        vault_action_close = BrokerActionEnd(self.broker_url,vault_login_token).end_action()
        print(f"Action ends with return code {vault_action_close}")

        #broker_close_intention = BrokerCloseIntention(self.broker_url,intention_response['intention_token']).close_intention()

        return {
            'secrets': secrets,
            'action_token': vault_database_token,
            'intention_token': intention_response['intention_token'],
        }
