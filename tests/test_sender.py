import time

import pytest
from fakes import FakeProducer, FakeRedis

from src.ingestion.dedup import Deduplicator
from src.ingestion.sender import send_batch


def rec(tx_id, **over):
    r = {"transaction_id": tx_id, "customer_id": 7, "amount": 50.0, "currency": "USD", "timestamp": int(time.time() * 1000)}
    r.update(over)
    return r


def setup():
    redis = FakeRedis()
    return redis, Deduplicator(redis)


def test_unique_records_are_all_sent_and_confirmed():
    redis, dedup = setup()
    producer = FakeProducer()
    stats = send_batch(producer, dedup, [rec("a"), rec("b"), rec("c")], "t")
    assert stats == {"sent": 3, "duplicate": 0, "invalid": 0, "failed": 0}
    assert len(producer.produced) == 3
    assert all(v[0] == "sent" for v in redis.store.values())


def test_duplicate_in_same_batch_is_sent_only_once():
    _, dedup = setup()
    producer = FakeProducer()
    stats = send_batch(producer, dedup, [rec("a"), rec("a"), rec("b")], "t")
    assert stats["sent"] == 2 and stats["duplicate"] == 1
    assert [p["transaction_id"] for p in producer.produced] == ["a", "b"]


def test_duplicate_across_runs_is_blocked():
    _, dedup = setup()
    send_batch(FakeProducer(), dedup, [rec("a")], "t")
    producer2 = FakeProducer()
    stats = send_batch(producer2, dedup, [rec("a"), rec("z")], "t")
    assert stats["duplicate"] == 1 and stats["sent"] == 1
    assert [p["transaction_id"] for p in producer2.produced] == ["z"]


def test_failed_delivery_releases_key_so_retry_works():
    redis, dedup = setup()
    with pytest.raises(RuntimeError, match="failed delivery"):
        send_batch(FakeProducer(fail_ids={"a"}), dedup, [rec("a")], "t")
    assert "nexuslake:tx:a" not in redis.store
    stats = send_batch(FakeProducer(), dedup, [rec("a")], "t")
    assert stats["sent"] == 1


def test_produce_exception_releases_key():
    redis, dedup = setup()
    with pytest.raises(BufferError):
        send_batch(FakeProducer(raise_on_produce_ids={"a"}), dedup, [rec("a")], "t")
    assert "nexuslake:tx:a" not in redis.store


def test_invalid_record_is_skipped_and_never_reserved():
    redis, dedup = setup()
    producer = FakeProducer()
    stats = send_batch(producer, dedup, [rec("bad", amount=-5), rec("ok")], "t")
    assert stats["invalid"] == 1 and stats["sent"] == 1
    assert "nexuslake:tx:bad" not in redis.store


def test_all_invalid_raises():
    _, dedup = setup()
    with pytest.raises(ValueError):
        send_batch(FakeProducer(), dedup, [rec("a", amount=-1), rec("b", currency="XXX")], "t")


def test_undelivered_messages_after_flush_raise():
    _, dedup = setup()
    with pytest.raises(RuntimeError, match="not delivered"):
        send_batch(FakeProducer(leave_in_queue=2), dedup, [rec("a")], "t")
