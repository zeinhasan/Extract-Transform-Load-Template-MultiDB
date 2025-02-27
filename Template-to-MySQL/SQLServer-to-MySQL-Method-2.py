from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import pyodbc
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
    'sqlserver_to_mysql_etl',
    default_args=default_args,
    description='ETL DAG for transferring data from SQL Server to MySQL',
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

# SQL Server Connection and Data Extraction
def extract_data():
    try:
        conn_str = (
            "Driver={ODBC Driver 13 for SQL Server};"
            "Server=0.0.0.0;"  # Change to your server
            "Database=XXX;"  # Change database
            "UID=XX;"  # Change username
            "PWD=XXXXXX;"  # Change password
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
        send_email("ETL Job Failure - Extraction", str(e))
        raise

# Delete records older than 30 days in MySQL
def delete_old_records():
    try:
        conn = mysql.connector.connect(
            host="your-mysql-host",
            user="your-username",
            password="your-password",
            database="your-database"
        )
        cursor = conn.cursor()
        query = "DELETE FROM your_table WHERE date_trans < DATE_SUB(CURDATE(), INTERVAL 30 DAY)"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Old records deleted successfully.")
    except Exception as e:
        send_email("ETL Job Failure - Delete Step", str(e))
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

        # Assuming the table schema matches the dataframe
        cols = ",".join(df.columns)
        values_placeholder = ",".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO your_table ({cols}) VALUES ({values_placeholder})"

        delete_old_records()  # Cleanup before loading new data

        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()

        send_email("ETL Job Success", f"Successfully loaded {len(df)} records.")
    except Exception as e:
        send_email("ETL Job Failure - Load Step", str(e))
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
