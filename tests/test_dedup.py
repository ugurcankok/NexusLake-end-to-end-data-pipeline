from fakes import FakeRedis

from src.ingestion.dedup import Deduplicator, PENDING, SENT


def make():
    r = FakeRedis()
    return r, Deduplicator(r, ttl_seconds=3600, pending_ttl_seconds=300)


def test_first_reserve_wins_second_is_duplicate():
    _, d = make()
    assert d.reserve("tx-1") is True
    assert d.reserve("tx-1") is False


def test_reserve_uses_atomic_set_nx_with_prefix_and_short_ttl():
    r, d = make()
    d.reserve("tx-1")
    call = r.set_calls[0]
    assert call["nx"] is True
    assert call["key"] == "nexuslake:tx:tx-1"
    assert call["ex"] == 300
    assert r.store["nexuslake:tx:tx-1"][0] == PENDING


def test_confirm_keeps_key_with_full_ttl():
    r, d = make()
    d.reserve("tx-1")
    d.confirm("tx-1")
    assert r.store["nexuslake:tx:tx-1"] == (SENT, 3600)
    assert d.reserve("tx-1") is False, "teslim edilen kayıt TTL boyunca tekrar kabul edilmemeli"


def test_release_allows_retry():
    _, d = make()
    d.reserve("tx-1")
    d.release("tx-1")
    assert d.reserve("tx-1") is True


def test_different_ids_do_not_conflict():
    _, d = make()
    assert d.reserve("a") and d.reserve("b")
