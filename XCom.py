from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_etl_dag',
    default_args=default_args,
    description='Twitter scraping DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to extract data
def extract_data(**kwargs):
    """Simulates data extraction."""
    extracted_data = {'data': [1, 2, 3, 4, 5]}
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)
    logging.info("Data extracted: %s", extracted_data)

# Function to transform data
def transform_data(**kwargs):
    """Transforms extracted data by doubling each value."""
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(key='extracted_data')
    transformed_data = [x * 2 for x in extracted_data['data']]
    ti.xcom_push(key='transformed_data', value=transformed_data)
    logging.info("Data transformed: %s", transformed_data)

# Function to load data
def load_data(**kwargs):
    """Loads transformed data (simulated)."""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key
