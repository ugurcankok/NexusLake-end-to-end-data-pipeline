import os
import sys
import logging
import redis
from pathlib import Path
from src.ingestion.generator import generate_batch
from src.ingestion.dedup import Deduplicator
from src.ingestion.sender import send_batch
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NexusProducer")

TOPIC = 'nexus_transactions'
BATCH_SIZE = int(os.getenv("NEXUS_BATCH_SIZE", "10"))
DUPLICATE_RATE = float(os.getenv("NEXUS_DUPLICATE_RATE", "0.2"))

# Kafka Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'broker:29092',
    'enable.idempotence': True,
    'acks': 'all'
}

def get_redis_client():
    try:
        client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        client.ping()
        return client
    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        raise

def run_producer():
    logger.info("Producer is starting...")
    dedup = Deduplicator(get_redis_client())

    try:
        schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

        current_script_path = Path(__file__).resolve()
        src_root = current_script_path.parent.parent  # .../src
        schema_path = src_root / "schemas" / "transaction_schema.avsc"

        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found: {schema_path}")
            return

        with open(schema_path, "r") as f:
            schema_str = f.read()

        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        producer = SerializingProducer({**KAFKA_CONF, 'value.serializer': avro_serializer})

        records = generate_batch(BATCH_SIZE, DUPLICATE_RATE)
        stats = send_batch(producer, dedup, records, TOPIC)

        logger.info(
            f"The operation is complete. Generated {len(records)}: "
            f"{stats['sent']} sent, {stats['duplicate']} duplicate(s) blocked, {stats['invalid']} invalid skipped."
        )

    except Exception as e:
        logger.error(f"A critical error occurred while Producer was running: {str(e)}")
        raise

if __name__ == "__main__":
    run_producer()
