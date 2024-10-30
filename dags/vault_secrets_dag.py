import re
import json
from airflow.models import Variable
from airflow import DAG
from pendulum import datetime
from airflow.operators.python import PythonOperator
from operators.vault_operators import VaultSecretRequestOperator
from operators.broker_action_start import BrokerActionStart
from operators.broker_action_end import BrokerActionEnd
from operators.broker_intention_close import BrokerCloseIntention
from airflow.models.connection import Connection
from airflow.utils.session import provide_session
from airflow.providers.oracle.operators.oracle import OracleOperator

#from airflow.hooks.base_hook import BaseHook
#from airflow.operators.email_operator import EmailOperator
#from airflow.operators.dummy_operator import DummyOperator

SQL_FILE_PATH = '/opt/airflow/dags/sql/datafix_select.sql'
role_id = Variable.get("role_id", default_var=None)
broker_jwt = Variable.get("broker_jwt", default_var=None)

@provide_session
def create_oracle_connection(session=None,**kwargs):
    secrets = kwargs['ti'].xcom_pull(task_ids='fetch_secrets')
    if not secrets:
        raise Exception("No secrets found in XCom!")
    # Retrieve secrets from vault
    db_username = secrets['secrets'].get("db_username")
    db_password = secrets['secrets'].get("db_password")
    jdbc_oracle_url = secrets['secrets'].get("url")
    print(f"secrets from vault retrieved - id:{db_username}")
    pattern = pattern = r"jdbc:oracle:thin:@(?P<hostname>[^:]+):(?P<port>\d+)/(?P<service_name>.+)"
    match = re.match(pattern, jdbc_oracle_url)
    if match:
        # Extract hostname, port, and service name
        hostname = match.group('hostname')
        port = match.group('port')
        service_name = match.group('service_name')

        print(f"Hostname: {hostname}")
        print(f"Port: {port}")
        print(f"Service Name: {service_name}")
    else:
        print("Invalid JDBC string format")

    # dsn_tns = oracledb.makedsn(hostname, port, service_name=service_name)

    conn_id = "oracle_conn"
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    service_dict = {
        "service_name": service_name
    }
    service_str = json.dumps(service_dict)
    if not existing_conn:
        conn = Connection(
            conn_id=conn_id,
            conn_type="oracle",
            host=hostname,
            login=db_username,  # Oracle username
            password=db_password,  # Oracle password
            port=port,
            extra=service_str
        )
        session.add(conn)
        session.commit()
        print(f"Oracle connection {conn_id} created.")
    else:
        print(f"Oracle DSN connection {conn_id} already exists.")

# Read the SQL query from the file
def read_sql_from_file(file_path):
    with open(file_path, 'r') as file:
        sql_commands = file.read().strip().split(';')
        return [command.strip() for command in sql_commands if command.strip()]

# dynamic_task for oracle queries
def dynamic_task(sql_command, task_id_prefix):
    run_query_task = OracleOperator(
        task_id=f'{task_id_prefix}_run_query',
        sql=sql_command,
        oracle_conn_id="oracle_conn",
        autocommit=True,
    )

    check_results_task = PythonOperator(
        task_id=f'{task_id_prefix}_check_results',
        python_callable=check_results,
        op_kwargs={'task_id': f'{task_id_prefix}_run_query'},
    )

    run_query_task >> check_results_task  # Ensure check runs after query

    return [run_query_task, check_results_task]

# format email content for notification
def format_email(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='run_sql_query')
    service_name = kwargs['dag'].default_args['service_name']
    sysdate = result[0]
    subject = f"{service_name} - Query Results: "
    content = f"The query returned the following result:\n\nCurrent Date: {sysdate}"

    #subject = f"{service_name} - Query Results: {len(result)} rows found"
    #content = f"The query returned the following results:\n\n" + "\n".join([str(row) for row in result])
    kwargs['ti'].xcom_push(key='email_subject', value=subject)
    kwargs['ti'].xcom_push(key='email_content', value=content)

def check_results(task_id, **kwargs):
    results = kwargs['ti'].xcom_pull(task_ids=task_id)
    if results:
        print('send email out')
        return 'send_email'
    print('no email needed')
    return 'no_email'

def database_action_start(**kwargs):
    database_actions_tokens = kwargs['ti'].xcom_pull(task_ids='fetch_secrets')
    if not database_actions_tokens:
        raise Exception("No secrets found in XCom!")

    action_action = database_actions_tokens.get("action_token")
    datafix_action_start = BrokerActionStart(default_args['broker_url'],action_action).start_action()
    return datafix_action_start

def database_actions_end(**kwargs):
    database_actions_tokens = kwargs['ti'].xcom_pull(task_ids='fetch_secrets')
    if not database_actions_tokens:
        raise Exception("No secrets found in XCom!")
    action_action = database_actions_tokens.get("action_token")
    datafix_action_end = BrokerActionEnd(default_args['broker_url'],action_action).end_action()
    return datafix_action_end

def database_intention_close(**kwargs):
    database_actions_tokens = kwargs['ti'].xcom_pull(task_ids='fetch_secrets')
    if not database_actions_tokens:
        raise Exception("No secrets found in XCom!")
    intention_action = database_actions_tokens.get("intention_token")
    datafix_intention_close = BrokerCloseIntention(default_args['broker_url'],intention_action).close_intention()
    return datafix_intention_close

sql_commands = read_sql_from_file(SQL_FILE_PATH)

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 23),
    'retries': 1,
    'service_name': 'rrs-automatedb-datafix',
    'vault_url': 'https://knox.io.nrs.gov.bc.ca',
    'broker_url': 'https://broker.io.nrs.gov.bc.ca',
}

with DAG('vault_secrets_dag',
        default_args=default_args,
        schedule_interval=None,
) as dag:
    # Task to get secrets from Vault
    fetch_secrets_task  = VaultSecretRequestOperator(
        task_id='fetch_secrets',
        project_name='rrs',
        service_name=default_args['service_name'],
        vault_url=default_args['vault_url'],
        broker_url=default_args['broker_url'],
        target_env='development',
        role_id=role_id,
        broker_jwt=broker_jwt,
        subpath='db_proxy_datafix'
    )

    create_connection_task = PythonOperator(
        task_id='create_oracle_connection',
        python_callable=create_oracle_connection,
    )

    datacheck_start_task = PythonOperator(
        task_id='access_database_start',
        python_callable=database_action_start,
        provide_context=True
    )

    previous_tasks = []  # List to keep track of the previous tasks

    for idx, sql_command in enumerate(sql_commands):
        task_id_prefix = f"task_{idx}"
        task_group = dynamic_task(sql_command, task_id_prefix)
        previous_tasks.extend(task_group)

    datacheck_end_task = PythonOperator(
        task_id='access_database_end',
        python_callable=database_actions_end,
        provide_context=True
    )

    end_task = PythonOperator(
        task_id='intention_end',
        python_callable=database_intention_close,
        provide_context=True
    )

    # Set task dependencies
    fetch_secrets_task >> create_connection_task
    for i, task in enumerate(previous_tasks):
        if i == 0:
            create_connection_task >> datacheck_start_task >> task
        else:
            previous_tasks[i - 1] >> task
    previous_tasks[-1] >> datacheck_end_task >> end_task
