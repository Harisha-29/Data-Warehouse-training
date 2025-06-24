from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

def check_file_size():
    size = os.path.getsize("data/inventory.csv")
    return 'task_a' if size < 500 * 1024 else 'task_b'

with DAG('inventory_branch_dag', start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    branch = BranchPythonOperator(task_id='branching', python_callable=check_file_size)
    
    task_a = BashOperator(task_id='task_a', bash_command='echo "Light Summary Task"')
    task_b = BashOperator(task_id='task_b', bash_command='echo "Detailed Processing Task"')
    
    cleanup = BashOperator(task_id='cleanup', bash_command='echo "Common Cleanup"')

    branch >> [task_a, task_b] >> cleanup
