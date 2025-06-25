from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys

def validate_data():
    path = '/opt/airflow/data/orders.csv'
    required_columns = ['order_id', 'product', 'quantity', 'price']
    df = pd.read_csv(path)

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    if df[required_columns].isnull().any().any():
        raise ValueError("Nulls found in mandatory fields")

def summarize_data():
    print("Summarizing data...")

with DAG(
    dag_id="data_quality_validation",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    validate = PythonOperator(
        task_id="validate_csv_data",
        python_callable=validate_data
    )

    summarize = PythonOperator(
        task_id="summarize_data",
        python_callable=summarize_data
    )

    validate >> summarize
