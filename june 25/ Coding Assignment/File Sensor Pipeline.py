from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import shutil
import os

def process_file():
    print("File processing logic here...")

def move_to_archive():
    source = '/opt/airflow/data/incoming/report.csv'
    destination = '/opt/airflow/data/archive/report.csv'
    shutil.move(source, destination)

with DAG(
    dag_id="file_sensor_pipeline",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_report",
        filepath="data/incoming/report.csv",
        fs_conn_id='fs_default',
        timeout=600,
        poke_interval=30,
        mode='poke',
        soft_fail=True
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file
    )

    archive = PythonOperator(
        task_id="move_file_to_archive",
        python_callable=move_to_archive
    )

    wait_for_file >> process >> archive
