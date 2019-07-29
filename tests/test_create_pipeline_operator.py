import datetime
import json
import sys
from airflow import DAG
from bedrock_plugin import BedrockHook, CreatePipelineOperator

if sys.version_info >= (3, 3):
    from unittest.mock import PropertyMock, call, patch
else:
    from mock import PropertyMock, call, patch


def test_create_pipeline(airflow_connection):
    name = "some_name"
    uri = "some_uri"
    ref = "some_ref"
    username = "some_username"
    password = "some_password"
    config_file_path = "bedrock.hcl"

    dag = DAG("bedrock_dag", start_date=datetime.datetime.now())
    op = CreatePipelineOperator(
        task_id="create_pipeline",
        dag=dag,
        conn_id=airflow_connection,
        name=name,
        uri=uri,
        ref=ref,
        username=username,
        password=password,
        config_file_path=config_file_path,
    )

    resp = PropertyMock()
    resp.content = '{"public_id": "some_id"}'
    with patch.object(BedrockHook, "run", return_value=resp) as mock_resp:
        op.execute(None)

    data = json.dumps(
        {
            "name": name,
            "uri": uri,
            "ref": ref,
            "username": username,
            "password": password,
            "config_file_path": config_file_path,
        }
    )
    assert mock_resp.mock_calls == [
        call(CreatePipelineOperator.CREATE_PIPELINE_PATH, data=data)
    ]