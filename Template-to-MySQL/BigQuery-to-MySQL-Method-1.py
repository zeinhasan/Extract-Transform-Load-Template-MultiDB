from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import mysql.connector
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
    'bigquery_to_mysql_etl',
    default_args=default_args,
    description='ETL DAG for BigQuery to MySQL data transfer',
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

# BigQuery Extraction
def extract_data():
    try:
        query = "SELECT * FROM `your_project.your_dataset.your_table`"
        df = bq_client.query(query).to_dataframe()
        df = df.replace([float('inf'), float('-inf')], None)
        return df
    except Exception as e:
        send_email("ETL Job Failure", str(e))
        raise

# Load Data into MySQL
def load_data(**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(task_ids='extract_data')
        conn = mysql.connector.connect(
            host="your-mysql-host",
            user="your-username",
            password="your-password",
            database="your-database"
        )
        cursor = conn.cursor()

        # Assuming your table schema matches the dataframe
        cols = ",".join(df.columns)
        values_placeholder = ",".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO your_table ({cols}) VALUES ({values_placeholder})"

        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()
        
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
