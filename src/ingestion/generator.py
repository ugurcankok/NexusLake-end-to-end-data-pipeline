import uuid
import random
import time

def generate_mock_transaction():
    # The fields must correspond exactly to src/schemas/transaction_schema.avsc.
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(100, 999),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "TRY"]),
        "timestamp": int(time.time() * 1000), 
    }


def generate_batch(size=10, duplicate_rate=0.0, rng=None):
    rng = rng or random
    batch = []
    for _ in range(size):
        if batch and rng.random() < duplicate_rate:
            batch.append(dict(rng.choice(batch)))
        else:
            batch.append(generate_mock_transaction())
    return batch
