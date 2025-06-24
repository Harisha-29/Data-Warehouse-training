from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import random

def flaky_api():
    if random.random() < 0.7: 
        raise ValueError("Random failure")
    print("API call succeeded")

default_args = {
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}

with DAG('retry_alert_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    call_api = PythonOperator(task_id='call_flaky_api', python_callable=flaky_api)

    alert = BashOperator(
        task_id='alert_failure',
        bash_command='echo "API failed after retries!"',
        trigger_rule='one_failed'
    )

    success_task = BashOperator(
        task_id='post_success_task',
        bash_command='echo "Only run if API succeeded!"',
        trigger_rule='all_success'
    )

    call_api >> [alert, success_task]
