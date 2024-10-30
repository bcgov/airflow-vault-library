from airflow.plugins_manager import AirflowPlugin
from operators.broker_util import BrokerUtils
from operators.broker_intention_open import BrokerOpenIntention
from operators.broker_action_start import BrokerActionStart
from operators.broker_vault_login import VaultLoginProcessor
from operators.broker_action_end import BrokerActionEnd
from operators.broker_intention_close import BrokerCloseIntention
from operators.vault_operators import VaultSecretRequestOperator

class MyCustomPlugin(AirflowPlugin):
    name = "my_custom_plugin"
    operators = [BrokerUtils, BrokerOpenIntention, BrokerActionStart, VaultLoginProcessor, BrokerActionEnd, BrokerCloseIntention, VaultSecretRequestOperator]