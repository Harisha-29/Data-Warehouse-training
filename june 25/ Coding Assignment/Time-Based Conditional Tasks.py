from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import datetime

def decide_task():
    now = datetime.datetime.now()
    weekday = now.weekday()
    hour = now.hour

    if weekday >= 5:
        return "skip_dag"
    elif hour < 12:
        return "morning_task"
    else:
        return "afternoon_task"

def task_a():
    print("Morning Task A running...")

def task_b():
    print("Afternoon Task B running...")

with DAG(
    dag_id="time_based_conditional_tasks",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    check_time = BranchPythonOperator(
        task_id="decide_path",
        python_callable=decide_task
    )

    morning = PythonOperator(
        task_id="morning_task",
        python_callable=task_a
    )

    afternoon = PythonOperator(
        task_id="afternoon_task",
        python_callable=task_b
    )

    skip = DummyOperator(task_id="skip_dag")
    cleanup = DummyOperator(task_id="cleanup", trigger_rule="none_failed_or_skipped")

    check_time >> [morning, afternoon, skip] >> cleanup
