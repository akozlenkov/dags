from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Функция, которую будем запускать
def hello_world(**kwargs):
    print("Hello, Airflow 3.1.7!")
    return "Done"

# Определяем DAG
with DAG(
    dag_id="python_operator_example",
    start_date=datetime(2026, 2, 27),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        provide_context=True,  # для доступа к kwargs
    )

    task_hello

