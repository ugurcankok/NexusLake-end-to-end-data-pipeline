import logging

from src.quality.rules import validate_record

logger = logging.getLogger("NexusProducer")


def send_batch(producer, dedup, records, topic, flush_timeout=30):
    """Returns: {"sent", "duplicate", "invalid", "failed"} counters. 

    Throws an exception in case of error (the Airflow task fails and tries again).
    """
    stats = {"sent": 0, "duplicate": 0, "invalid": 0, "failed": 0}

    def make_callback(tx_id):
        def on_delivery(err, msg):
            if err is not None:
                logger.error(f"Delivery failed for {tx_id}: {err}")
                dedup.release(tx_id)
                stats["failed"] += 1
            else:
                dedup.confirm(tx_id)
                stats["sent"] += 1
        return on_delivery

    for rec in records:
        tx_id = rec.get("transaction_id")

        problems = validate_record(rec)
        if problems:
            logger.warning(f"Invalid record skipped ({'; '.join(problems)}): {tx_id}")
            stats["invalid"] += 1
            continue

        if not dedup.reserve(tx_id):
            logger.warning(f"Duplicate blocked: {tx_id}")
            stats["duplicate"] += 1
            continue

        try:
            producer.produce(topic=topic, value=rec, on_delivery=make_callback(tx_id))
        except Exception:
            dedup.release(tx_id)  # could not be added to the queue: do not release the lock
            raise
        producer.poll(0)  # Run callbacks for completed deliveries

    remaining = producer.flush(flush_timeout)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) were not delivered before the flush timeout")
    if stats["failed"]:
        raise RuntimeError(f"{stats['failed']} message(s) failed delivery")
    if stats["sent"] == 0 and stats["duplicate"] == 0 and stats["invalid"] > 0:
        raise ValueError("All generated records were invalid; check the generator and quality rules.")
    return stats
