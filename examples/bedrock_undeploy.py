from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from bedrock_train import JsonHttpOperator, API_VERSION, CONN_ID, HEADERS


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 7, 24),
}

with DAG(
    dag_id="bedrock_deploy",
    default_args=args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    # on_failure_callback=lambda context: trigger_dag("bedrock_dag_v2")
) as dag:
    deploy = TriggerDagRunOperator(
        trigger_dag_id="bedrock_deploy",
        retries=3,
        retry_delay=timedelta(seconds=5)
    )

    get_endpoint = JsonHttpOperator(
        task_id="get_endpoint",
        http_conn_id=CONN_ID,
        endpoint="{}/endpoint/{}".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='deploy_model')['public_id'] }}"
        ),
        method="GET",
        headers=HEADERS,
        response_check=lambda response: len(response.json()["deployments"]) > 0,
        xcom_push=True,
    )

    def keep_latest(**kwargs):
        deployments = kwargs["ti"].xcom_pull(task_ids="get_endpoint")["deployments"]
        past_models = sorted(deployments, key=lambda d: d["created_at"])[:-1]
        return [JsonHttpOperator(
            task_id="undeploy_model",
            http_conn_id=CONN_ID,
            endpoint="{}/serve/id/{}/deploy".format(
                API_VERSION, model["entity_id"]),
            method="DELETE",
            headers=HEADERS,
            response_check=lambda response: response.status_code == 202,
        ) for model in past_models]
    keep_latest = PythonOperator(
        task_id="keep_latest",
        python_callable=keep_latest,
        provide_context=True,
    )

    check_endpoint = HttpSensor(
        task_id="check_endpoint",
        http_conn_id=CONN_ID,
        endpoint="{}/endpoint/{}".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='deploy_model')['public_id'] }}"
        ),
        method="GET",
        response_check=lambda response: len(response.json()["deployments"]) == 1,
        poke_interval=10,
        timeout=300,
    )

    deploy >> keep_latest >> check_endpoint
