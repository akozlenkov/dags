from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

def hello_world(**kwargs):
    print("Hello from KubernetesExecutor with pod_override and volumes!")

with DAG(
    dag_id="python_operator_k8s_with_volumes",
    start_date=datetime(2026, 2, 27),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Pod override с volumes и env из secrets
    pod_override = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="apache/airflow:3.1.7-python3.12",
#                    command=["bash", "-c"],
#                    args=["exec airflow worker"],  # можно заменить на любой запуск
 #                   env=[
 #                       k8s.V1EnvVar(
 #                           name="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
 #                           value_from=k8s.V1EnvVarSource(
 #                               secret_key_ref=k8s.V1SecretKeySelector(
 #                                   name="my-airflow-database",
 #                                   key="connection"
 #                               )
 #                           )
 #                       ),
 #                       k8s.V1EnvVar(
 #                           name="AIRFLOW__CORE__FERNET_KEY",
 #                           value_from=k8s.V1EnvVarSource(
 #                               secret_key_ref=k8s.V1SecretKeySelector(
 #                                   name="my-airflow-fernet-key",
 #                                   key="fernet-key"
 #                               )
 #                           )
 #                       ),
 #                       k8s.V1EnvVar(
 #                           name="AIRFLOW__API_AUTH__JWT_SECRET",
 #                           value_from=k8s.V1EnvVarSource(
 #                               secret_key_ref=k8s.V1SecretKeySelector(
 #                                   name="my-airflow-jwt-secret",
 #                                   key="jwt-secret"
 #                               )
 #                           )
 #                       ),
 #                   ],
 #                   volume_mounts=[
 #                       k8s.V1VolumeMount(
 #                           name="config",
 #                           mount_path="/opt/airflow/airflow.cfg",
 #                           sub_path="airflow.cfg",
 #                           read_only=True,
 #                       )
 #                   ],
                )
            ],
 #           volumes=[
 #               k8s.V1Volume(
 #                   name="config",
 #                   config_map=k8s.V1ConfigMapVolumeSource(
 #                       name="my-airflow-config",
 #                       default_mode=0o644
 #                   )
 #               )
 #           ],
            service_account_name="airflow",  # при необходимости
        ),
        metadata=k8s.V1ObjectMeta(labels={"release": "stable"}),
    )

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        executor_config={"pod_override": pod_override},
    )

    end = EmptyOperator(task_id="end")

    start >> task_hello >> end

