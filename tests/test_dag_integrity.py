import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.databricks")
pytest.importorskip("confluent_kafka")

from airflow.models import DagBag  # noqa: E402
from pathlib import Path  # noqa: E402

DAGS = str(Path(__file__).resolve().parent.parent / "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_transformation_dag_order(dagbag):
    dag = dagbag.get_dag("nexuslake_transformation_v1")
    assert [t.task_id for t in dag.topological_sort()] == ["run_bronze_ingest", "run_silver_transform", "run_gold_transform"]


def test_ingestion_dag_schedule(dagbag):
    dag = dagbag.get_dag("nexuslake_ingestion_v1")
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.catchup is False
