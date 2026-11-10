import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GoldTransform")

try:
    SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError: 
    SRC_DIR = sys.argv[1]
sys.path.append(SRC_DIR)
try:
    from utils.helpers import (
        safe,
        SILVER_PATH, 
        GOLD_CUSTOMER_PATH, 
        GOLD_CURRENCY_PATH
    )
    from quality.rules import reconcile, DataQualityError
except ImportError as e:
    logger.error(f"Helpers import error: {e}")
    raise

def run_gold_transformation():
    logger.info("Spark session is starting (Gold Layer)...")

    spark = SparkSession.builder \
        .appName("NexusLake-Gold-Transformation") \
        .getOrCreate()

    athena_confs = {
        "spark.databricks.delta.properties.defaults.minReaderVersion": "1",
        "spark.databricks.delta.properties.defaults.minWriterVersion": "2",
        "spark.databricks.delta.properties.defaults.columnMapping.mode": "none",
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false",
    }
    for key, value in athena_confs.items():
        try:
            spark.conf.set(key, value)
        except Exception as e:
            logger.warning(f"Config could not be set on this compute ({key}): {e}")

    logger.info("The Gold Transformation Process Has Begun")

    try:
        logger.info(f"Reading the Silver data from the source:: {safe(SILVER_PATH)}")
        silver_df = spark.read.format("delta").load(SILVER_PATH)

        #  Analysis 1: Total Spending by Customer
        logger.info("Customer spending summary is being calculated...")
        customer_summary = silver_df.groupBy("customer_id") \
            .agg(
                sum("amount").alias("total_spent"),
                count("transaction_id").alias("transaction_count"),
                avg("amount").alias("avg_transaction_value")
            )

        # Analysis 2: Currency-Based Statistics
        logger.info("Currency statistics are being calculated...")
        currency_summary = silver_df.groupBy("currency") \
            .agg(
                sum("amount").alias("total_volume"),
                count("transaction_id").alias("total_count")
            )

        # Data quality: If the summaries do not match the Silver layer, do NOT write to the Gold layer (to prevent corrupt data from reaching Athena).
        silver_stats = silver_df.agg(count("*").alias("n"), sum("amount").alias("total")).collect()[0]
        customer_stats = customer_summary.agg(sum("transaction_count").alias("n"), sum("total_spent").alias("total")).collect()[0]
        currency_stats = currency_summary.agg(sum("total_count").alias("n"), sum("total_volume").alias("total")).collect()[0]
        problems = reconcile(
            silver_stats["n"], silver_stats["total"],
            customer_stats["n"], customer_stats["total"],
            currency_stats["n"], currency_stats["total"],
        )
        if problems:
            raise DataQualityError("Gold quality check failed: " + "; ".join(problems))
        logger.info(f"Gold quality check passed ({silver_stats['n']} silver rows reconciled).")

        # Overwrite the Gold Layer
        logger.info(f"Customer summary is being generated: {safe(GOLD_CUSTOMER_PATH)}")
        customer_summary.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_CUSTOMER_PATH)
        
        logger.info(f"Foreign exchange statistics are being compiled: {safe(GOLD_CURRENCY_PATH)}")
        currency_summary.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_CURRENCY_PATH)

        logger.info("Gold Transformation Successfully Completed")

    except Exception as e:
        logger.error(f"Gold layer transformation error: {str(e)}")
        raise e
    finally:
        logger.info("The Spark session is closing...")
        spark.stop()

if __name__ == "__main__":
    run_gold_transformation()