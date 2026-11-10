import sys
import os
import logging
from pyspark.sql import SparkSession

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BronzeIngest")

try:
    SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:  
    SRC_DIR = sys.argv[1]
sys.path.append(SRC_DIR)
try:
    from utils.helpers import (
        safe,
        KAFKA_BOOTSTRAP_SERVERS, 
        KAFKA_TOPIC, 
        BRONZE_PATH, 
        CHECKPOINT_BRONZE
    )
except ImportError as e:
    logger.error(f"Helpers could not be imported. Path: {sys.path}. Error: {e}")
    raise

def run_bronze_streaming():
    logger.info("Spark session is starting...")
    
    spark = SparkSession.builder \
        .appName("Nexus-Bronze-Ingest") \
        .getOrCreate()

    logger.info(f"Data retrieval from Kafka is starting. Server: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")

    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": KAFKA_TOPIC,
        "startingOffsets": "earliest",
        "kafka.security.protocol": "PLAINTEXT",
        "kafka.group.id": "nexus_bronze_ingest_group"
    }

    try:
        raw_df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()

        logger.info(f"The write operation is starting (Delta Format). Target: {safe(BRONZE_PATH)}")

        query = raw_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_BRONZE) \
            .trigger(availableNow=True) \
            .start(BRONZE_PATH)

        query.awaitTermination()
        logger.info("The write operation to the Bronze tier was successfully completed.")

    except Exception as e:
        logger.error(f"An error occurred during the bronze transformation: {str(e)}")
        raise

if __name__ == "__main__":
    run_bronze_streaming()