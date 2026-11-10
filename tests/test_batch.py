import random

from src.ingestion.generator import generate_batch


def ids(batch):
    return [r["transaction_id"] for r in batch]


def test_batch_without_duplicates_is_all_unique():
    batch = generate_batch(50, duplicate_rate=0.0)
    assert len(batch) == 50 and len(set(ids(batch))) == 50


def test_batch_with_full_duplicate_rate_repeats_first_record():
    batch = generate_batch(10, duplicate_rate=1.0, rng=random.Random(1))
    assert len(batch) == 10 and len(set(ids(batch))) == 1


def test_duplicates_are_exact_copies_not_same_object():
    batch = generate_batch(10, duplicate_rate=1.0, rng=random.Random(1))
    assert batch[0] == batch[1] and batch[0] is not batch[1]


def test_some_duplicates_with_partial_rate():
    batch = generate_batch(200, duplicate_rate=0.3, rng=random.Random(42))
    assert 0 < 200 - len(set(ids(batch))) < 200
