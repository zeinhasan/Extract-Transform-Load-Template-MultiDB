from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2  # PostgreSQL
import pymysql  # MySQL
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgresql_to_mysql_etl',
    default_args=default_args,
    description='ETL DAG for PostgreSQL to MySQL data transfer',
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

# PostgreSQL Connection and Data Extraction
def extract_data():
    try:
        conn = psycopg2.connect(
            host="your-postgres-host",
            user="your-username",
            password="your-password",
            database="your-database",
            port=5432  # Default PostgreSQL port
        )
        query = "SELECT * FROM your_table WHERE date_trans >= CURRENT_DATE - INTERVAL '30 days'"
        df = pd.read_sql(query, conn)
        df = df.replace([float('inf'), float('-inf')], None)
        conn.close()
        return df
    except Exception as e:
        send_email("ETL Job Failure", str(e))
        raise

# Delete records older than 30 days in MySQL
def delete_old_records():
    try:
        conn = pymysql.connect(
            host="your-mysql-host",
            user="your-username",
            password="your-password",
            database="your-database"
        )
        cursor = conn.cursor()
        query = "DELETE FROM your_table WHERE date_trans < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Old records deleted successfully.")
    except Exception as e:
        send_email("ETL Job Failure - Delete Step", str(e))
        raise

# MySQL Data Loading
def load_data(**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(task_ids='extract_data')
        conn = pymysql.connect(
            host="your-mysql-host",
            user="your-username",
            password="your-password",
            database="your-database"
        )
        cursor = conn.cursor()
        
        delete_old_records()  # Cleanup before loading new data
        
        for _, row in df.iterrows():
            sql = """
            INSERT INTO your_table (col1, col2, col3, date_trans) 
            VALUES (%s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE col1=VALUES(col1), col2=VALUES(col2), col3=VALUES(col3), date_trans=VALUES(date_trans)
            """
            cursor.execute(sql, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        send_email("ETL Job Success", f"Successfully loaded {len(df)} records into MySQL.")
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