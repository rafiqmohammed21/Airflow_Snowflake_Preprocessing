from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from scipy import stats

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'snowflake_anomaly_detection',
    default_args=default_args,
    description='Detect anomalies in Snowflake tables',
    schedule_interval='@daily',
)

# Function to fetch data from Snowflake
def fetch_data_from_snowflake(table_name, datetime_column, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    sql = f"SELECT * FROM {table_name} WHERE {datetime_column} >= DATEADD(day, -30, CURRENT_DATE())"
    df = hook.get_pandas_df(sql)
    # Save data to XCom
    kwargs['ti'].xcom_push(key='fetched_data', value=df.to_json())

# Function to detect anomalies
def detect_anomalies(**kwargs):
    fetched_data = kwargs['ti'].xcom_pull(key='fetched_data', task_ids='fetch_data')
    df = pd.read_json(fetched_data)
    numeric_columns = df.select_dtypes(include=np.number).columns.tolist()
    anomalies = {}
    
    for col in numeric_columns:
        z_scores = np.abs(stats.zscore(df[col]))
        anomalies[col] = df[z_scores > 3]
    
    # Save anomalies to XCom
    kwargs['ti'].xcom_push(key='anomalies', value=anomalies)
    
    # Send notifications if anomalies are found
    if any(len(anomaly) > 0 for anomaly in anomalies.values()):
        send_slack_alert(anomalies)

# Function to send Slack alerts
def send_slack_alert(anomalies):
    slack_token = Variable.get("slack_token")
    client = WebClient(token=slack_token)
    
    for col, anomaly in anomalies.items():
        if len(anomaly) > 0:
            message = f"Anomalies detected in column {col}: {anomaly.to_json()}"
            try:
                response = client.chat_postMessage(channel='#alerts', text=message)
            except SlackApiError as e:
                print(f"Error sending message: {e.response['error']}")

# Define tasks
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_snowflake,
    op_args=['{{ dag_run.conf["table_name"] }}', '{{ dag_run.conf["datetime_column"] }}'],
    provide_context=True,
    dag=dag,
)

detect_anomalies = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_data >> detect_anomalies
