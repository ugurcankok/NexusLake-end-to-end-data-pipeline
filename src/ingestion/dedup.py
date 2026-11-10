PENDING = "pending"
SENT = "sent"


class Deduplicator:
    def __init__(self, client, ttl_seconds=3600, pending_ttl_seconds=300, prefix="nexuslake:tx:"):
        self.client = client
        self.ttl = ttl_seconds                  # for how long should a delivered record be considered "viewed"?
        self.pending_ttl = pending_ttl_seconds  # If the process crashes, any incomplete reservation will be automatically deleted after a certain period.
        self.prefix = prefix

    def _key(self, tx_id: str) -> str:
        return f"{self.prefix}{tx_id}"

    def reserve(self, tx_id: str) -> bool:
        """Returns True if the record is encountered for the first time (and reserves it), or False if it has been seen before. This operation is atomic."""
        return bool(self.client.set(self._key(tx_id), PENDING, nx=True, ex=self.pending_ttl))

    def confirm(self, tx_id: str) -> None:
        """Message acknowledged by Kafka: set the key's time-to-live (TTL) to 'sent'."""
        self.client.set(self._key(tx_id), SENT, ex=self.ttl)

    def release(self, tx_id: str) -> None:
        """Delivery failed: delete the reservation so that the record can be resubmitted."""
        self.client.delete(self._key(tx_id))
