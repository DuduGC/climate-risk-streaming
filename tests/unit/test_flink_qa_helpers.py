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
        lambda path: {"name": job_name, "state": "FAILED"},
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
    assert canceled_ids == []


def test_startup_rest_failure_cleans_the_job_id_already_stored_in_submission_holder(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-cleanuprestfailure"
    job_id = "abcdef0123456789abcdef0123456789"
    submission: dict[str, str | None] = {"job_id": None}
    returned_id: str | None = None
    canceled: list[str] = []
    states = iter(("RUNNING", "CANCELLING", "CANCELED"))
    rest_calls = 0

    monkeypatch.setattr(
        temporal_helpers.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command,
            0,
            stdout=f"JOB_ID={job_id}",
            stderr="",
        ),
    )

    def rest_json(path: str) -> dict[str, str]:
        nonlocal rest_calls
        rest_calls += 1
        if rest_calls == 1:
            raise TimeoutError("REST polling failed after JobID capture")
        return {"name": job_name, "state": next(states)}

    monkeypatch.setattr(temporal_helpers, "_rest_json", rest_json)
    monkeypatch.setattr(temporal_helpers, "_cancel_own_active_job", canceled.append)
    monkeypatch.setattr(temporal_helpers.time, "sleep", lambda seconds: None)

    with pytest.raises(TimeoutError, match="REST polling failed"):
        try:
            returned_id = temporal_helpers._submit_unbounded_qa_job(
                job_name,
                f"{job_name}-group",
                submission=submission,
            )
        finally:
            temporal_helpers._cleanup_unbounded_qa_job(
                job_name,
                returned_id,
                submission,
            )

    assert returned_id is None
    assert submission["job_id"] == job_id
    assert canceled == [job_id]
    assert rest_calls == 4


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
    states = iter(("RUNNING", "CANCELLING", "CANCELED"))
    monkeypatch.setattr(
        temporal_helpers,
        "_rest_json",
        lambda path: {"name": job_name, "state": next(states)},
    )
    monkeypatch.setattr(temporal_helpers.time, "sleep", lambda seconds: None)

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


def test_unbounded_submission_can_opt_into_business_sinks_and_alternate_qa_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-businessfault"
    group_id = f"{job_name}-group"
    job_id = "0123456789abcdef0123456789abcdef"
    entrypoint = "/tmp/weather-flink-qa-fault-jdbc.py"
    captured: dict[str, object] = {}
    submission: dict[str, str | None] = {"job_id": None}

    def run(command, **kwargs):
        captured["command"] = command
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=f"JOB_ID={job_id}",
            stderr="",
        )

    monkeypatch.setattr(temporal_helpers.subprocess, "run", run)
    monkeypatch.setattr(temporal_helpers, "_rest_json", lambda path: {"state": "RUNNING"})

    result = temporal_helpers._submit_unbounded_qa_job(
        job_name,
        group_id,
        submission=submission,
        business_sinks=True,
        python_entrypoint=entrypoint,
    )

    command = captured["command"]
    entrypoint_index = command.index("-py")
    assert command[entrypoint_index + 1] == entrypoint
    assert "--qa-audit" in command
    assert "--qa-business-sinks" in command
    assert submission["job_id"] == job_id
    assert result == job_id


@pytest.mark.parametrize(
    "entrypoint",
    [
        "/tmp/untrusted.py",
        "/tmp/weather-flink-qa-no-extension",
        "/tmp/weather-flink-qa-../untrusted.py",
        "relative/weather-flink-qa-untrusted.py",
    ],
)
def test_unbounded_submission_rejects_alternate_entrypoints_outside_qa_tmp_namespace(
    entrypoint: str,
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-entrypointgate"
    subprocess_calls: list[object] = []
    monkeypatch.setattr(
        temporal_helpers.subprocess,
        "run",
        lambda *args, **kwargs: subprocess_calls.append(args) or None,
    )

    with pytest.raises(ValueError, match="/tmp/weather-flink-qa-"):
        temporal_helpers._submit_unbounded_qa_job(
            job_name,
            f"{job_name}-group",
            submission={"job_id": None},
            business_sinks=True,
            python_entrypoint=entrypoint,
        )

    assert subprocess_calls == []


@pytest.mark.parametrize(
    ("job_name", "group_id"),
    [
        ("weather-kafka-ingestion", "weather-kafka-ingestion"),
        ("weather-flink-qa-businessid", "weather-flink-qa-other-group"),
        ("weather-flink-qa-two-parts", "weather-flink-qa-two-parts-group"),
    ],
)
def test_business_qa_submission_rejects_nonexclusive_job_and_group_before_docker(
    job_name: str,
    group_id: str,
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    subprocess_calls: list[object] = []
    monkeypatch.setattr(
        temporal_helpers.subprocess,
        "run",
        lambda *args, **kwargs: subprocess_calls.append(args) or None,
    )

    with pytest.raises(ValueError, match="weather-flink-qa"):
        temporal_helpers._submit_unbounded_qa_job(
            job_name,
            group_id,
            submission={"job_id": None},
            business_sinks=True,
        )

    assert subprocess_calls == []


def test_unbounded_qa_cleanup_waits_for_its_own_job_to_reach_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_name = "weather-flink-qa-terminalcleanup"
    job_id = "fedcba9876543210fedcba9876543210"
    states = iter(("RUNNING", "CANCELLING", "CANCELED"))
    canceled: list[str] = []
    looked_up: list[tuple[str, str | None]] = []

    monkeypatch.setattr(
        temporal_helpers,
        "_wait_for_own_job_id",
        lambda name, known_id: looked_up.append((name, known_id)) or known_id,
    )
    monkeypatch.setattr(
        temporal_helpers,
        "_rest_json",
        lambda path: {"name": job_name, "state": next(states)},
    )
    monkeypatch.setattr(temporal_helpers, "_cancel_own_active_job", canceled.append)
    monkeypatch.setattr(temporal_helpers.time, "sleep", lambda seconds: None)

    temporal_helpers._cleanup_unbounded_qa_job(
        job_name,
        job_id,
        {"job_id": None},
    )

    assert looked_up == [(job_name, job_id)]
    assert canceled == [job_id]


def test_unbounded_qa_cleanup_refuses_to_cancel_a_job_with_a_different_name(
    monkeypatch: pytest.MonkeyPatch,
    temporal_helpers: ModuleType,
) -> None:
    job_id = "fedcba9876543210fedcba9876543210"
    canceled: list[str] = []
    monkeypatch.setattr(
        temporal_helpers,
        "_wait_for_own_job_id",
        lambda name, known_id: known_id,
    )
    monkeypatch.setattr(
        temporal_helpers,
        "_rest_json",
        lambda path: {"name": "weather-kafka-ingestion", "state": "RUNNING"},
    )
    monkeypatch.setattr(temporal_helpers, "_cancel_own_active_job", canceled.append)

    with pytest.raises(AssertionError, match="refusing to clean up unexpected job"):
        temporal_helpers._cleanup_unbounded_qa_job(
            "weather-flink-qa-cleanupgate",
            job_id,
            {"job_id": None},
        )

    assert canceled == []
