from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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
    description='Twitter scrapping tag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to extract data
def extract_data(**kwargs):
    # Simulate data extraction
    extracted_data = {'data': [1, 2, 3, 4, 5]}
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)
    print("Data extracted:", extracted_data)

# Function to transform data
def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(key='extracted_data')
    transformed_data = [x * 2 for x in extracted_data['data']]
    ti.xcom_push(key='transformed_data', value=transformed_data)
    print("Data transformed:", transformed_data)

# Function to load data
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data')
    # Simulate loading data
    print("Data loaded:", transformed_data)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> load_task
