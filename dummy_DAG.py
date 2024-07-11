# simple DAG with clear task dependencies, using dummy and Python operators, which can be extended with more complex logic and resource configurations.
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def sample_task(**kwargs):
    print("Task executed")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=sample_task,
        provide_context=True,
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=sample_task,
        provide_context=True,
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> [task_1, task_2] >> end
