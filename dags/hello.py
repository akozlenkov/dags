from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Определяем DAG
with DAG(
    dag_id="dummy_example",
    start_date=datetime(2026, 2, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    task_1 = DummyOperator(
        task_id="task_1"
    )

    task_2 = DummyOperator(
        task_id="task_2"
    )

    end = DummyOperator(
        task_id="end"
    )

    # Определяем последовательность
    start >> [task_1, task_2] >> end

