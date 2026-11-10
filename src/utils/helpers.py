import os
import re
import urllib.parse

SECRET_SCOPE = "nexuslake"


def _get_secret(key: str, env_name: str, default: str = None) -> str:
    try:
        from databricks.sdk.runtime import dbutils
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        pass 
    value = os.environ.get(env_name, default)
    if value is None:
        raise RuntimeError(
            f"Secret not found: '{key}' must be defined within the Databricks secret scope '{SECRET_SCOPE}', "
            f"or the environment variable {env_name} must be set."
        )
    return value


# AWS Credentials
AWS_ACCESS_KEY = _get_secret("aws_access_key", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = _get_secret("aws_secret_key", "AWS_SECRET_KEY")
AWS_BUCKET_NAME = _get_secret("aws_bucket_name", "AWS_BUCKET_NAME", default="nexuslake")

# Kafka Configuration 
KAFKA_BOOTSTRAP_SERVERS = _get_secret("kafka_bootstrap_servers", "KAFKA_BOOTSTRAP_SERVERS", default="localhost:9092")
KAFKA_TOPIC = "nexus_transactions"

# S3 Path Calculation (Base Path)
encoded_secret = urllib.parse.quote(AWS_SECRET_KEY, safe='')
BASE_S3_PATH = f"s3a://{AWS_ACCESS_KEY}:{encoded_secret}@{AWS_BUCKET_NAME}"

# Layer Paths
BRONZE_PATH = f"{BASE_S3_PATH}/bronze/transactions"
SILVER_PATH = f"{BASE_S3_PATH}/silver/transactions"
QUARANTINE_PATH = f"{BASE_S3_PATH}/quarantine/transactions" 
GOLD_CUSTOMER_PATH = f"{BASE_S3_PATH}/gold/customer_spending_summary"
GOLD_CURRENCY_PATH = f"{BASE_S3_PATH}/gold/currency_stats"

# Checkpoint Paths
CHECKPOINT_BRONZE = f"{BASE_S3_PATH}/checkpoints/bronze_v1"
CHECKPOINT_SILVER = f"{BASE_S3_PATH}/checkpoints/silver_v1"
CHECKPOINT_QUARANTINE = f"{BASE_S3_PATH}/checkpoints/quarantine_v1"
CHECKPOINT_GOLD = f"{BASE_S3_PATH}/checkpoints/gold_v1"


def safe(path: str) -> str:
    """Masks the credentials within `s3a://KEY:SECRET@bucket` when writing to the log."""
    return re.sub(r"s3a://[^@/]+@", "s3a://", path)
