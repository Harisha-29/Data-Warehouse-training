from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json

def parse_response(**kwargs):
    response = kwargs['ti'].xcom_pull(task_ids='get_api_data')
    data = json.loads(response)
    print("API Data:", data)

with DAG(
    dag_id="external_api_interaction",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    get_data = SimpleHttpOperator(
        task_id="get_api_data",
        http_conn_id="http_default",
        endpoint="v1/bpi/currentprice.json",  # For example, coindesk BTC price
        method="GET",
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        dag=dag
    )

    parse = PythonOperator(
        task_id="parse_api_response",
        python_callable=parse_response,
        provide_context=True
    )

    get_data >> parse
