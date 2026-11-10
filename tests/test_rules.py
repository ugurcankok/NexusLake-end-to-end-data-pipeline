import pytest

from src.quality.rules import validate_record, reconcile

NOW = 1_800_000_000_000  # sabit "şimdi" (ms)


def good(**over):
    rec = {"transaction_id": "abc-1", "customer_id": 42, "amount": 100.5, "currency": "EUR", "timestamp": NOW - 1000}
    rec.update(over)
    return rec


def test_valid_record_has_no_problems():
    assert validate_record(good(), now_ms=NOW) == []


@pytest.mark.parametrize("field,value", [
    ("transaction_id", ""),
    ("transaction_id", None),
    ("customer_id", 0),
    ("customer_id", "42"),
    ("amount", 0),
    ("amount", -5.0),
    ("amount", 10_000_000.0),
    ("amount", None),
    ("currency", "GBP"),
    ("currency", None),
    ("timestamp", 0),
    ("timestamp", NOW + 10 * 24 * 3600 * 1000),
])
def test_invalid_field_is_reported(field, value):
    problems = validate_record(good(**{field: value}), now_ms=NOW)
    assert len(problems) == 1 and problems[0].startswith(field)


def test_multiple_problems_are_all_reported():
    assert len(validate_record(good(amount=-1, currency="XXX"), now_ms=NOW)) == 2


def test_reconcile_ok():
    assert reconcile(10, 1000.0, 10, 1000.0, 10, 1000.0) == []


def test_reconcile_detects_count_and_total_mismatch():
    problems = reconcile(10, 1000.0, 9, 1000.0, 10, 900.0)
    assert len(problems) == 2
    assert any("customer_spending_summary" in p for p in problems)
    assert any("currency_stats" in p for p in problems)


def test_reconcile_empty_silver_fails():
    problems = reconcile(0, None, 0, None, 0, None)
    assert len(problems) == 1
    assert "silver" in problems[0].lower()
