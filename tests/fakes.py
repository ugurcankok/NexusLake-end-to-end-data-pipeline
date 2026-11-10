"""Testler için sahte Redis ve sahte Kafka producer."""


class FakeRedis:
    def __init__(self):
        self.store = {}   # key -> (value, ttl)
        self.set_calls = []

    def set(self, key, value, nx=False, ex=None):
        self.set_calls.append({"key": key, "nx": nx, "ex": ex})
        if nx and key in self.store:
            return None
        self.store[key] = (value, ex)
        return True

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


class FakeProducer:
    """produce() kaydı kuyruğa alır; poll/flush callback'leri çalıştırır."""

    def __init__(self, fail_ids=(), raise_on_produce_ids=(), leave_in_queue=0):
        self.fail_ids = set(fail_ids)
        self.raise_on_produce_ids = set(raise_on_produce_ids)
        self.leave_in_queue = leave_in_queue
        self.produced = []
        self._queue = []

    def produce(self, topic, value, on_delivery):
        if value["transaction_id"] in self.raise_on_produce_ids:
            raise BufferError("queue full")
        self.produced.append(value)
        self._queue.append((value, on_delivery))

    def _deliver_all(self):
        queue, self._queue = self._queue, []
        for value, cb in queue:
            if value["transaction_id"] in self.fail_ids:
                cb("boom", None)
            else:
                cb(None, object())

    def poll(self, timeout):
        self._deliver_all()
        return 0

    def flush(self, timeout=None):
        self._deliver_all()
        return self.leave_in_queue
