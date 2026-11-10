import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator


DATABRICKS_REPO_PATH = os.getenv("NEXUS_DATABRICKS_REPO_PATH", "/Workspace/Repos/CHANGE_ME/NexusLake")
DATABRICKS_CLUSTER_ID = os.getenv("NEXUS_DATABRICKS_CLUSTER_ID", "CHANGE_ME")
DATABRICKS_CONN_ID = "databricks_default"

default_args = {
    'owner': 'ugurcankok',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'databricks_conn_id': DATABRICKS_CONN_ID,
}


def databricks_task(task_id: str, script: str) -> DatabricksSubmitRunOperator:
    """Runs a PySpark script within a repository on Databricks Serverless compute."""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        json={
            "run_name": f"nexuslake_{task_id}",
            "tasks": [
                {
                    "task_key": task_id,
                    "spark_python_task": {
                        "python_file": f"{DATABRICKS_REPO_PATH}/src/processing/{script}",
                        "parameters": [f"{DATABRICKS_REPO_PATH}/src"],
                    },
                    "environment_key": "default",
                }
            ],
            "environments": [
                {"environment_key": "default", "spec": {"environment_version": "2"}}
            ],
        },
    )

with DAG(
    'nexuslake_transformation_v1',
    default_args=default_args,
    description='Daily Bronze to Gold Spark Pipeline (Databricks)',
    schedule_interval='0 17 * * *',  # Every day 17:00
    catchup=False,
    tags=['nexuslake', 'databricks', 'gold', 'silver']
) as dag:

    # 1. Kafka -> Bronze
    bronze_task = databricks_task('run_bronze_ingest', 'bronze_transform.py')

    # 2. Bronze -> Silver (Avro parse & cast)
    silver_task = databricks_task('run_silver_transform', 'silver_transform.py')

    # 3. Silver -> Gold (Analytics)
    gold_task = databricks_task('run_gold_transform', 'gold_transform.py')

    bronze_task >> silver_task >> gold_task
