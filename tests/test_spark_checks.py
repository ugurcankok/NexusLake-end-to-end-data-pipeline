"""Silver kalite koşulu Spark'ta doğru çalışıyor mu? (pyspark ve Java kuruluysa çalışır)"""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from quality.spark_checks import valid_condition  # noqa: E402
import time  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_valid_condition_splits_rows(spark):
    now = int(time.time() * 1000)
    rows = [
        ("ok-1", 1, 10.0, "USD", now),       # geçerli
        ("bad-amount", 1, -1.0, "USD", now),  # negatif tutar
        ("bad-cur", 1, 10.0, "GBP", now),     # desteklenmeyen para birimi
        (None, 1, 10.0, "USD", now),          # NULL id
        ("bad-ts", 1, 10.0, "USD", 0),        # eski timestamp
    ]
    df = spark.createDataFrame(rows, "transaction_id string, customer_id int, amount double, currency string, timestamp long")
    valid = [r.transaction_id for r in df.filter(valid_condition()).collect()]
    assert valid == ["ok-1"]
