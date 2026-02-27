from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def hello_world(**kwargs):
    print("Hello from KubernetesExecutor!")

with DAG(
    dag_id="python_operator_k8s111",
    start_date=datetime(2026, 2, 27),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        executor_config={
            "KubernetesExecutor": {
                "image": "apache/airflow:3.1.7-python3.12"
            }
        },
    )

    end = EmptyOperator(task_id="end")

    start >> task_hello >> end


