parent_dag.py
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG(
    dag_id="parent_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start_task")

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="child_dag",
        conf={"trigger_date": str(datetime.now())}
    )

    start >> trigger_child
child_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def run_child_task(**kwargs):
    print("Triggered by parent DAG.")
    print("Received conf:", kwargs['dag_run'].conf)

with DAG(
    dag_id="child_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="child_task",
        python_callable=run_child_task,
        provide_context=True
    )
