import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.http_sensor import HttpSensor
from bedrock_train import JsonHttpOperator, API_VERSION, CONN_ID, HEADERS, run_options


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
    train = TriggerDagRunOperator(
        trigger_dag_id="bedrock_train",
        retries=3,
        retry_delay=timedelta(seconds=5)
    )

    check_auc = JsonHttpOperator(
        task_id="check_auc",
        http_conn_id=CONN_ID,
        endpoint="{}/training_run/{}".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='run_pipeline')['entity_id'] }}"
        ),
        method="GET",
        headers=HEADERS,
        response_check=lambda response: response.json()["metrics"]["AUC"] > 0.92,
        xcom_push=True,
    )

    deploy_model = JsonHttpOperator(
        task_id="deploy_model",
        http_conn_id=CONN_ID,
        endpoint="{}/serve/deploy".format(API_VERSION),
        method="POST",
        data=json.dumps(
            {
                "pipeline_run_id": "{{ ti.xcom_pull(task_ids='run_pipeline')[0]['public_id'] }}",
                "public_id": run_options["pipeline_public_id"]
            }
        ),
        headers=HEADERS,
        response_check=lambda response: response.status_code == 202,
        xcom_push=True,
    )

    def is_success(response):
        status = response.json()["status"]
        if status == "Deployed":
            return True
        if status in ["Stopped", "Failed", "Error"]:
            raise Exception("Deployment failed: {}".format(response))
        return False
    check_server = HttpSensor(
        task_id="check_server",
        http_conn_id=CONN_ID,
        endpoint="{}/serve/id/{}".format(
            API_VERSION, "{{ ti.xcom_pull(task_ids='deploy_model')['entity_id'] }}"
        ),
        method="GET",
        response_check=is_success,
        poke_interval=10,
        timeout=300,
    )

    train >> check_auc >> deploy_model >> check_server
