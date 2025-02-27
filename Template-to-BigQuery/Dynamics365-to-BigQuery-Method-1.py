from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import adal
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import time

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'd365_odata_demand_forecasts',
    default_args=default_args,
    description='ETL DAG for D365 Demand Forecasts',
    schedule_interval='@daily',
    catchup=False
)

# Credentials & Configuration
CLIENT_ID = 'your-client-id'
CLIENT_SECRET = 'your-client-secret'
TENANT_ID = 'your-tenant-id'
RESOURCE = 'https://your-d365-url.com'
API_ENDPOINT = f'{RESOURCE}/data/DemandForecasts'
AUTHORITY_URL = f'https://login.microsoftonline.com/{TENANT_ID}'

BQ_PROJECT = 'your-project-id'
BQ_DATASET_TABLE = 'your-dataset.your-table'

SMTP_SERVER = 'your-smtp-server'
SENDER_EMAIL = 'your-email@example.com'
RECIPIENT_EMAILS = ['recipient@example.com']

def send_email(subject, body):
    try:
        server = smtplib.SMTP(SMTP_SERVER, 25)
        msg = MIMEText(body)
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
        server.quit()
    except Exception as e:
        print(f'Email notification failed: {e}')

def extract_data():
    context = adal.AuthenticationContext(AUTHORITY_URL)
    token_response = context.acquire_token_with_client_credentials(
        RESOURCE, CLIENT_ID, CLIENT_SECRET
    )
    headers = {
        'Authorization': f'Bearer {token_response["accessToken"]}',
        'OData-Version': '4.0',
        'Accept': 'application/json'
    }
    response = requests.get(API_ENDPOINT, headers=headers)
    if response.status_code == 200:
        data = response.json().get('value', [])
        return data
    else:
        raise RuntimeError(f'Failed to fetch data: {response.status_code}')

def transform_data(**context):
    data = context['task_instance'].xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(data)
    df.drop(columns=['@odata.etag'], errors='ignore', inplace=True)
    return df

def load_to_bigquery(**context):
    df = context['task_instance'].xcom_pull(task_ids='transform_data')
    credentials = service_account.Credentials.from_service_account_file('path-to-key.json')
    client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df, BQ_DATASET_TABLE, job_config=job_config)
    job.result()
    return len(df)

def notify_success(**context):
    records = context['task_instance'].xcom_pull(task_ids='load_to_bigquery')
    send_email('ETL Success', f'Successfully processed {records} records.')

def notify_failure(context):
    send_email('ETL Failure', f'ETL job failed: {context}')

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    provide_context=True,
    dag=dag,
)

dag.on_failure_callback = notify_failure

extract_task >> transform_task >> load_task >> success_task