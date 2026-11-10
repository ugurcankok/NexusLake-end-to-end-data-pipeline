from pyspark.sql import functions as F
from quality.rules import ALLOWED_CURRENCIES, MIN_AMOUNT, MAX_AMOUNT, MIN_TIMESTAMP_MS, MAX_FUTURE_MS


def valid_condition():
    now_ms = F.unix_timestamp() * 1000
    cond = (
        F.col("transaction_id").isNotNull() & (F.length(F.trim(F.col("transaction_id"))) > 0)
        & F.col("customer_id").isNotNull() & (F.col("customer_id") > 0)
        & F.col("amount").isNotNull() & F.col("amount").between(MIN_AMOUNT, MAX_AMOUNT)
        & F.col("currency").isin(*ALLOWED_CURRENCIES)
        & F.col("timestamp").isNotNull()
        & (F.col("timestamp") >= MIN_TIMESTAMP_MS) & (F.col("timestamp") <= now_ms + MAX_FUTURE_MS)
    )
    return F.coalesce(cond, F.lit(False))
