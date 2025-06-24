from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import pandas as pd

default_args = {
    'start_date': datetime(2023, 1, 1),
}

def check_file():
    file_path = 'data/customers.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")

def count_rows(ti):
    df = pd.read_csv('data/customers.csv')
    row_count = len(df)
    ti.xcom_push(key='row_count', value=row_count)
    print(f"Total rows: {row_count}")

def decide_next(ti):
    return 'send_alert' if ti.xcom_pull(key='row_count') > 100 else 'done'

with DAG('csv_summary_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    check = PythonOperator(task_id='check_file', python_callable=check_file)
    count = PythonOperator(task_id='count_rows', python_callable=count_rows)
    
    send_alert = BashOperator(task_id='send_alert', bash_command='echo "Row count exceeds 100!"')
    done = BashOperator(task_id='done', bash_command='echo "Done"')
    
    from airflow.operators.python import BranchPythonOperator
    decide = BranchPythonOperator(task_id='branch_logic', python_callable=decide_next)

    check >> count >> decide >> [send_alert, done]
