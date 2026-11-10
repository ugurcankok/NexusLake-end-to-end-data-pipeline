import sys
import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.avro.functions import from_avro

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SilverTransform")

try:
    SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:  
    SRC_DIR = sys.argv[1]
sys.path.append(SRC_DIR)
try:
    from utils.helpers import (
        safe,
        BRONZE_PATH, 
        SILVER_PATH, 
        QUARANTINE_PATH,
        CHECKPOINT_SILVER,
        CHECKPOINT_QUARANTINE
    )
    from quality.spark_checks import valid_condition
except ImportError as e:
    logger.error(f"Helpers import error: {e}")
    raise

def run_silver_transformation():
    logger.info("Spark session is starting...")

    spark = SparkSession.builder \
        .appName("NexusLake-Silver-Transformation") \
        .getOrCreate()

    schema_path = Path(SRC_DIR) / "schemas" / "transaction_schema.avsc"
    
    logger.info(f"Loading the Avro chart: {schema_path}")

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            avro_schema_json = f.read()
    except FileNotFoundError:
        logger.error(f"ERROR: Schema file not found: {schema_path}")
        return

    try:
        logger.info(f"Reading the Bronze data: {safe(BRONZE_PATH)}")
        bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)
        
        # Magic byte removal and Avro parsing
        binary_df = bronze_df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
        
        parsed_df = binary_df.select(
            from_avro(col("fixed_value"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("parsed_data"),
            col("value").alias("raw_value")
        ).select("parsed_data.*", "raw_value")

        # Data quality: valid records go to the silver layer, invalid ones go to quarantine.
        checked_df = parsed_df.withColumn("_is_valid", valid_condition())

        silver_df = checked_df.filter(col("_is_valid")) \
            .drop("_is_valid", "raw_value") \
            .withColumn("event_timestamp", (col("timestamp") / 1000).cast("timestamp"))

        quarantine_df = checked_df.filter(~col("_is_valid")) \
            .drop("_is_valid") \
            .withColumn("quarantined_at", current_timestamp())

        logger.info(f"Writing to the Silver layer begins (Delta). Target: {safe(SILVER_PATH)}")
        silver_query = silver_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_SILVER) \
            .trigger(availableNow=True) \
            .start(SILVER_PATH)

        quarantine_query = quarantine_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_QUARANTINE) \
            .trigger(availableNow=True) \
            .start(QUARANTINE_PATH)

        silver_query.awaitTermination()
        quarantine_query.awaitTermination()

        try:
            bad_total = spark.read.format("delta").load(QUARANTINE_PATH).count()
            if bad_total > 0:
                logger.warning(f"Data quality: {bad_total} record(s) in quarantine so far ({safe(QUARANTINE_PATH)})")
        except Exception:
            logger.info("Data quality: no invalid records so far (quarantine table does not exist yet).")

        logger.info("The Silver Transformation has been successfully completed.")

    except Exception as e:
        logger.error(f"Error during the Silver transformation: {str(e)}")
        raise

if __name__ == "__main__":
    run_silver_transformation()