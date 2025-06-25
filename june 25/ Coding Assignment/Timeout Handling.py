from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time

def long_task():
    time.sleep(15)

with DAG(
    dag_id="retry_timeout_handling",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="long_running_task",
        python_callable=long_task,
        retries=3,
        retry_delay=10,
        retry_exponential_backoff=True,
        execution_timeout=timedelta(seconds=20)
    )
