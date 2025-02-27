from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import LoadJobConfig
import pandas as pd
import pyodbc
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import os

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'd365_inventtrans_etl',
    default_args=default_args,
    description='ETL DAG for NAMEDB NAMETABLE data',
    schedule_interval='@daily',
    catchup=False
)

# Email settings
SERVER_ADDRESS = "messenger.xxxxx.com"
PORT = 25
SENDER_EMAIL = "XXXX.office@XXXX.com"
RECIPIENT_EMAILS = ["recipient1@xxxx.com", "recipient2@xxxx.com"]

def send_email(subject, body):
    try:
        server = smtplib.SMTP(SERVER_ADDRESS, PORT)
        msg = MIMEText(body)
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
        server.quit()
        print("Email notification sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Load credentials
CREDENTIALS_PATH = "../service-account.json"
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# SQL Server Connection
def extract_data():
    try:
        conn_str = (
            "Driver={ODBC Driver 13 for SQL Server};"
            "Server=0.0.0.0;"
            "Database=XXX;"
            "UID=XX;"
            "PWD=XXXXXX;"
        )
        with pyodbc.connect(conn_str) as conn:
            query = """
            SELECT * FROM [DBNAME].[dbo].[DBTABLE]
            WHERE date_trans >= DATEADD(DAY, -30, GETDATE())
            """
            df = pd.read_sql(query, conn)
            df = df.replace([float('inf'), float('-inf')], None)
            return df
    except Exception as e:
        send_email("ETL Job Failure", str(e))
        raise

# Delete records older than 30 days in BigQuery
def delete_old_records():
    try:
        query = """
        DELETE FROM `Dataset.Tablename`
        WHERE date_trans < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        query_job = bq_client.query(query)
        query_job.result()
        print("Old records deleted successfully.")
    except Exception as e:
        send_email("ETL Job Failure - Delete Step", str(e))
        raise

# Load data into BigQuery
def load_data(**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(task_ids='extract_data')
        table_ref = "Dataset.Tablename"

        delete_old_records()  # Cleanup before loading new data

        job_config = LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND"
        )
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        send_email("ETL Job Success", f"Successfully loaded {len(df)} records.")
    except Exception as e:
        send_email("ETL Job Failure", str(e))
        raise

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
