import importlib

import pytest

import utils.helpers as helpers


def test_safe_hides_credentials_in_s3a_path():
    path = "s3a://AKIAXXXX:se%2Fcret@my-bucket/bronze/transactions"
    assert helpers.safe(path) == "s3a://my-bucket/bronze/transactions"


def test_safe_leaves_other_paths_untouched():
    assert helpers.safe("/Workspace/Users/x/NexusLake") == "/Workspace/Users/x/NexusLake"


def test_paths_are_built_from_environment(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY", "AKIAENV")
    monkeypatch.setenv("AWS_SECRET_KEY", "a/b+c")
    monkeypatch.setenv("AWS_BUCKET_NAME", "bucket-x")
    h = importlib.reload(helpers)
    assert h.BRONZE_PATH.startswith("s3a://AKIAENV:a%2Fb%2Bc@bucket-x/")
    assert "a/b+c" not in h.BRONZE_PATH, "secret URL içinde encode edilmeli"
    assert h.QUARANTINE_PATH.endswith("/quarantine/transactions")


def test_missing_secret_raises_clear_error(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY", raising=False)
    with pytest.raises(RuntimeError, match="AWS_ACCESS_KEY"):
        importlib.reload(helpers)
    monkeypatch.setenv("AWS_ACCESS_KEY", "AKIATESTKEY")
    importlib.reload(helpers)
