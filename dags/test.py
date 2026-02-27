from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

def hello_world(**kwargs):
    print("Hello from KubernetesExecutor with pod_override!")

with DAG(
    dag_id="python_operator_k8s111",
    start_date=datetime(2026, 2, 27),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Создаем pod_override с кастомным контейнером
    pod_override = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="apache/airflow:3.1.7-python3.12",  # <- здесь указываем образ
                    # можно добавить args, env, resources и т.д.
                )
            ]
        ),
        metadata=k8s.V1ObjectMeta(labels={"release": "stable"}),
    )

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        executor_config={"pod_override": pod_override},  # <-- используем pod_override
    )

    end = EmptyOperator(task_id="end")

    start >> task_hello >> end


