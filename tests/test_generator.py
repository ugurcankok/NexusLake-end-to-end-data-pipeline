import json
import time
from pathlib import Path

from src.ingestion.generator import generate_mock_transaction
from src.quality.rules import validate_record

SCHEMA = json.loads((Path(__file__).resolve().parent.parent / "src" / "schemas" / "transaction_schema.avsc").read_text())
AVRO_TO_PY = {"string": (str,), "int": (int,), "long": (int,), "double": (float,)}


def test_generated_record_matches_avro_schema():
    rec = generate_mock_transaction()
    expected = {f["name"]: f["type"] for f in SCHEMA["fields"]}
    assert set(rec) == set(expected), "generator alanları şemayla birebir aynı olmalı"
    for name, avro_type in expected.items():
        assert isinstance(rec[name], AVRO_TO_PY[avro_type]), f"{name} tipi {avro_type} olmalı"


def test_timestamp_is_in_milliseconds():
    rec = generate_mock_transaction()
    assert abs(rec["timestamp"] - time.time() * 1000) < 5_000


def test_transaction_ids_are_unique():
    ids = {generate_mock_transaction()["transaction_id"] for _ in range(500)}
    assert len(ids) == 500


def test_generated_records_pass_quality_rules():
    for _ in range(1000):
        assert validate_record(generate_mock_transaction()) == []
