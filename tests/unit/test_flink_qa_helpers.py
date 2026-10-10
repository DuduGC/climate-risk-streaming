from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def temporal_helpers(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    integration_directory = Path(__file__).resolve().parents[1] / "integration"
    monkeypatch.syspath_prepend(str(integration_directory))
    return importlib.import_module("test_flink_temporal")


def test_startup_failure_cleans_job_id_captured_before_rest_poll(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-cleanupknown"
    group_id = f"{job_name}-group"
    job_id = "0123456789abcdef0123456789abcdef"
    lookup_arguments: list[tuple[str, str | None]] = []
    canceled_ids: list[str] = []
    submission: dict[str, str | None] = {"job_id": None}
    result_job_id: str | None = None

    monkeypatch.setattr(
        temporal_helpers.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout=f"Job has been submitted with JobID {job_id}",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        temporal_helpers,
        "_rest_json",
        lambda path: {"state": "FAILED"},
    )

    def find_submitted_job(name: str, known_id: str | None) -> str | None:
        lookup_arguments.append((name, known_id))
        return known_id

    monkeypatch.setattr(temporal_helpers, "_wait_for_own_job_id", find_submitted_job)
    monkeypatch.setattr(temporal_helpers, "_cancel_own_active_job", canceled_ids.append)

    with pytest.raises(AssertionError, match="ended FAILED"):
        try:
            result_job_id = temporal_helpers._submit_unbounded_qa_job(
                job_name,
                group_id,
                submission=submission,
            )
        finally:
            temporal_helpers._cleanup_unbounded_qa_job(
                job_name,
                result_job_id,
                submission,
            )

    assert submission["job_id"] == job_id
    assert lookup_arguments == [(job_name, job_id)]
    assert canceled_ids == [job_id]


def test_cli_failure_finds_and_cancels_only_qa_job_by_unique_name(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-cleanuplookup"
    group_id = f"{job_name}-group"
    job_id = "fedcba9876543210fedcba9876543210"
    canceled_ids: list[str] = []
    submission: dict[str, str | None] = {"job_id": None}
    result_job_id: str | None = None
    legacy_helpers = importlib.import_module("test_kafka_flink")

    def fail_cli(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(returncode=1, cmd=args[0])

    monkeypatch.setattr(temporal_helpers.subprocess, "run", fail_cli)
    monkeypatch.setattr(
        legacy_helpers,
        "_jobs_overview",
        lambda: [
            {"name": "weather-flink-qa-other", "jid": "11111111111111111111111111111111"},
            {"name": job_name, "jid": job_id},
        ],
    )
    monkeypatch.setattr(temporal_helpers, "_cancel_own_active_job", canceled_ids.append)

    with pytest.raises(subprocess.CalledProcessError):
        try:
            result_job_id = temporal_helpers._submit_unbounded_qa_job(
                job_name,
                group_id,
                submission=submission,
            )
        finally:
            temporal_helpers._cleanup_unbounded_qa_job(
                job_name,
                result_job_id,
                submission,
            )

    assert submission["job_id"] is None
    assert canceled_ids == [job_id]
