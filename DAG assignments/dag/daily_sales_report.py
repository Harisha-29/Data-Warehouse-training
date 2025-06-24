from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import shutil
import os

def summarize_sales():
    df = pd.read_csv('data/sales.csv')
    summary = df.groupby('category')['amount'].sum().reset_index()
    summary.to_csv('data/sales_summary.csv', index=False)

def archive_file():
    shutil.move('data/sales.csv', f'archive/sales_{datetime.now().strftime("%Y%m%d%H%M")}.csv')

default_args = {
    'start_date': days_ago(1),
    'execution_timeout': timedelta(minutes=5),
}

with DAG('daily_sales_report', default_args=default_args, schedule_interval='0 6 * * *', catchup=False) as dag:
    summarize = PythonOperator(task_id='summarize_sales', python_callable=summarize_sales)
    archive = PythonOperator(task_id='archive_file', python_callable=archive_file)

    summarize >> archive
