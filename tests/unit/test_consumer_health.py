from __future__ import annotations

import json
import os

import pytest


def _health_module():
    import importlib

    try:
        return importlib.import_module("climate_logistics.consumer_health")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.consumer_health":
            pytest.fail("the consumer health module has not been implemented yet", pytrace=False)
        raise


@pytest.mark.unit
def test_write_health_atomically_writes_only_minimal_health_metadata(tmp_path):
    module = _health_module()
    path = tmp_path / "health.json"

    module.write_health(
        path,
        pid=os.getpid(),
        ready=True,
        updated_at=100.0,
        metadata_at=95.0,
    )

    assert json.loads(path.read_text(encoding="utf-8")) == {
        "pid": os.getpid(),
        "ready": True,
        "updated_at": 100.0,
        "metadata_at": 95.0,
    }
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.unit
def test_healthcheck_accepts_a_live_ready_process_with_recent_monotonic_times(tmp_path):
    module = _health_module()
    path = tmp_path / "health.json"
    module.write_health(
        path,
        pid=os.getpid(),
        ready=True,
        updated_at=100.0,
        metadata_at=95.0,
    )

    assert module.healthcheck(path, now=100.0) is True
    assert module.healthcheck(path, now=140.0) is True
    assert module.healthcheck(path, now=140.001) is False


@pytest.mark.unit
@pytest.mark.parametrize(
    "document",
    [
        "{not json",
        "[]",
        '{"pid":true,"ready":true,"updated_at":100,"metadata_at":99}',
        '{"pid":1,"ready":false,"updated_at":100,"metadata_at":99}',
        '{"pid":1,"ready":true,"updated_at":100,"metadata_at":101}',
        '{"pid":1,"ready":true,"updated_at":100,"metadata_at":NaN}',
        '{"pid":1,"ready":true,"updated_at":100,"metadata_at":99,"payload":"extra"}',
    ],
)
def test_healthcheck_rejects_corrupt_or_incoherent_health_files(tmp_path, document):
    module = _health_module()
    path = tmp_path / "health.json"
    path.write_text(document, encoding="utf-8")

    assert module.healthcheck(path, now=100.0) is False


@pytest.mark.unit
def test_healthcheck_rejects_missing_stale_future_and_dead_process_files(tmp_path):
    module = _health_module()
    path = tmp_path / "health.json"
    assert module.healthcheck(path, now=100.0) is False

    for pid, updated_at, metadata_at in (
        (os.getpid(), 54.0, 54.0),
        (os.getpid(), 101.0, 99.0),
        (2**30, 100.0, 99.0),
    ):
        module.write_health(
            path,
            pid=pid,
            ready=True,
            updated_at=updated_at,
            metadata_at=metadata_at,
        )
        assert module.healthcheck(path, now=100.0) is False


@pytest.mark.unit
def test_healthcheck_cli_returns_failure_without_a_ready_health_file(tmp_path, monkeypatch):
    module = _health_module()
    monkeypatch.setattr(module, "DEFAULT_HEALTH_PATH", tmp_path / "missing.json")

    assert module.main() == 1
