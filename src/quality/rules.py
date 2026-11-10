import time

ALLOWED_CURRENCIES = ("USD", "EUR", "TRY")
MIN_AMOUNT = 0.01
MAX_AMOUNT = 1_000_000.0
MIN_TIMESTAMP_MS = 1_577_836_800_000  # 2020-01-01
MAX_FUTURE_MS = 24 * 60 * 60 * 1000   
AMOUNT_TOLERANCE = 0.01


class DataQualityError(Exception):
    """Thrown when data quality check fails."""


def validate_record(rec: dict, now_ms: int = None) -> list:
    """Validates a transaction record. Returns a list of violations (if empty, the record is valid)."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    problems = []

    tx_id = rec.get("transaction_id")
    if not isinstance(tx_id, str) or not tx_id.strip():
        problems.append("transaction_id is empty")

    cid = rec.get("customer_id")
    if not isinstance(cid, int) or isinstance(cid, bool) or cid <= 0:
        problems.append("customer_id is invalid")

    amount = rec.get("amount")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool) or not (MIN_AMOUNT <= amount <= MAX_AMOUNT):
        problems.append("amount out of range")

    if rec.get("currency") not in ALLOWED_CURRENCIES:
        problems.append("currency not supported")

    ts = rec.get("timestamp")
    if not isinstance(ts, int) or isinstance(ts, bool) or not (MIN_TIMESTAMP_MS <= ts <= now_ms + MAX_FUTURE_MS):
        problems.append("timestamp is invalid")

    return problems


def reconcile(silver_count, silver_total, customer_count, customer_total,
              currency_count, currency_total, tolerance=AMOUNT_TOLERANCE) -> list:
    """Do the totals of the gold summaries match those of the silver summaries? It returns a list of discrepancies."""
    problems = []
    silver_count = silver_count or 0
    if silver_count == 0:
        return ["silver table is empty"]

    silver_total = silver_total or 0.0
    for name, cnt, tot in (("customer_spending_summary", customer_count, customer_total),
                           ("currency_stats", currency_count, currency_total)):
        cnt = cnt or 0
        tot = tot or 0.0
        if cnt != silver_count:
            problems.append(f"{name}: number of record {cnt}, silver {silver_count}")
        if abs(tot - silver_total) > tolerance * max(1, silver_count):
            problems.append(f"{name}: toplam tutar {tot:.2f}, silver {silver_total:.2f}")
    return problems
