from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
import os
import pandas as pd

@dag(schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False)
def dynamic_csv_dag():
    input_dir = 'data/input/'
    
    @task
    def list_files():
        return [f for f in os.listdir(input_dir) if f.endswith('.csv')]

    @task
    def process_file(file):
        df = pd.read_csv(os.path.join(input_dir, file))
        if not {'id', 'value'}.issubset(df.columns):
            raise ValueError(f"Invalid headers in {file}")
        summary = f"{file}: {len(df)} rows"
        with open("data/summary.txt", "a") as f:
            f.write(summary + "\n")
        return summary

    files = list_files()
    process_file.expand(file=files)

dynamic_csv_dag = dynamic_csv_dag()
