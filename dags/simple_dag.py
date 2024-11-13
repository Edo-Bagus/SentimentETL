from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, Apache Airflow!")

with DAG(
    'simple_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )
