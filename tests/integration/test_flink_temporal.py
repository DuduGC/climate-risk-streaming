from __future__ import annotations

import json
import os
import re
import subprocess
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

import pytest
from test_kafka_flink import (
    _JOB_ID_PATTERN,
    _PUBLISHER_SCRIPT,
    _cancel_own_active_job,
    _jobs_overview,
    _rest_json,
    _structured_log_entries,
    _taskmanager_logs_since,
    _wait_for_bounded_job,
    _wait_for_own_job_id,
    _wait_for_qa_audit_count,
)

pytestmark = pytest.mark.integration

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_REST_URL = "http://127.0.0.1:8081"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SLIDE_MS = 300_000
_WINDOW_MS = 1_800_000
_OUT_OF_ORDER_MS = 120_000
_ACTIVE_JOB_STATES = {"CREATED", "INITIALIZING", "RESTARTING", "RUNNING", "CANCELLING"}
_TERMINAL_JOB_STATES = {"CANCELED", "FAILED", "FINISHED"}
_QA_ENTRYPOINT_PATTERN = re.compile(r"/tmp/weather-flink-qa-[A-Za-z0-9][A-Za-z0-9_.-]*\.py\Z")


def _epoch_millis(value: datetime) -> int:
    elapsed = value.astimezone(UTC) - _EPOCH
    return elapsed.days * 86_400_000 + elapsed.seconds * 1_000 + elapsed.microseconds // 1_000


def _timestamp_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _weather_payload(
    location_id: str,
    event_timestamp: datetime,
    ingestion_timestamp: datetime,
    *,
    temperature: float,
    precipitation: float,
    measurement_interval_seconds: int,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
) -> str:
    return json.dumps(
        {
            "location_id": location_id,
            "city": "Temporal QA",
            "latitude": -22.9,
            "longitude": -43.17,
            "temperature": temperature,
            "precipitation": precipitation,
            "wind_speed": wind_speed,
            "wind_gust": wind_gust,
            "visibility": visibility,
            "weather_code": 61,
            "measurement_interval_seconds": measurement_interval_seconds,
            "event_timestamp": _timestamp_text(event_timestamp),
            "ingestion_timestamp": _timestamp_text(ingestion_timestamp),
            "source": "open-meteo",
            "schema_version": 1,
        },
        separators=(",", ":"),
    )


def _publish_weather(
    location_id: str,
    event_timestamp: datetime,
    ingestion_timestamp: datetime,
    *,
    temperature: float,
    precipitation: float,
    interval: int,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
) -> dict[str, str]:
    return {
        "key": location_id,
        "value": _weather_payload(
            location_id,
            event_timestamp,
            ingestion_timestamp,
            temperature=temperature,
            precipitation=precipitation,
            measurement_interval_seconds=interval,
            wind_speed=wind_speed,
            wind_gust=wind_gust,
            visibility=visibility,
        ),
    }


def _publish_temporal_records(records: list[dict[str, str]]) -> None:
    stdin = "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "producer",
            "python",
            "-c",
            _PUBLISHER_SCRIPT,
        ],
        cwd=_REPOSITORY_ROOT,
        input=stdin,
        check=True,
        capture_output=True,
        text=True,
        timeout=40,
    )
    assert f"DELIVERED={len(records)}" in result.stdout


def _expected_metric(
    *,
    location_id: str,
    start_ms: int,
    end_ms: int,
    count: int,
    temperature_avg: float,
    precipitation_max: float,
    precipitation_rate_max: float,
    wind_speed_max: float,
    wind_gust_max: float,
    visibility_min: float,
    interval_min: int,
    interval_max: int,
    first_ms: int,
    last_ms: int,
) -> dict[str, object]:
    return {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "event_count": count,
        "temperature_avg": temperature_avg,
        "precipitation_max": precipitation_max,
        "precipitation_rate_max_mm_per_hour": precipitation_rate_max,
        "wind_speed_max": wind_speed_max,
        "wind_gust_max": wind_gust_max,
        "visibility_min": visibility_min,
        "measurement_interval_min_seconds": interval_min,
        "measurement_interval_max_seconds": interval_max,
        "first_event_timestamp_ms": first_ms,
        "last_event_timestamp_ms": last_ms,
    }


def _metric_key(metric: dict[str, Any]) -> tuple[str, int, int]:
    return (
        str(metric["location_id"]),
        int(metric["window_start_ms"]),
        int(metric["window_end_ms"]),
    )


def _metric_records(logs: str, location_ids: set[str]) -> list[dict[str, Any]]:
    return [
        record
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "window_aggregate"
        and record.get("location_id") in location_ids
    ]


def _assert_metric_results(
    actual_records: list[dict[str, Any]],
    expected_by_key: dict[tuple[str, int, int], dict[str, object]],
) -> None:
    actual_by_key = {_metric_key(record): record for record in actual_records}
    assert len(actual_records) == len(actual_by_key), "metric sink emitted duplicate window results"
    assert actual_by_key.keys() == expected_by_key.keys()
    for key, expected in expected_by_key.items():
        actual = actual_by_key[key]
        assert actual.keys() == expected.keys()
        approximate_fields = {
            "temperature_avg",
            "precipitation_max",
            "precipitation_rate_max_mm_per_hour",
            "wind_speed_max",
            "wind_gust_max",
            "visibility_min",
        }
        for field in approximate_fields:
            assert actual[field] == pytest.approx(expected[field]), (
                f"unexpected {field} for {key}"
            )
        for field in expected.keys() - approximate_fields:
            assert actual[field] == expected[field], f"unexpected {field} for {key}"


def _expected_pair_windows(
    *,
    location_id: str,
    event_a_ms: int,
    event_b_ms: int,
    starts_ms: list[int],
) -> dict[tuple[str, int, int], dict[str, object]]:
    expected: dict[tuple[str, int, int], dict[str, object]] = {}
    for start_ms in starts_ms:
        includes_a = start_ms <= event_a_ms < start_ms + _WINDOW_MS
        includes_b = start_ms <= event_b_ms < start_ms + _WINDOW_MS
        if includes_a and includes_b:
            values = (2, 25.0, 2.0, 12.0, 50.0, 60.0, 6_000.0, 300, 900, event_b_ms, event_a_ms)
        elif includes_a:
            values = (1, 20.0, 2.0, 8.0, 40.0, 55.0, 9_000.0, 900, 900, event_a_ms, event_a_ms)
        elif includes_b:
            values = (1, 30.0, 1.0, 12.0, 50.0, 60.0, 6_000.0, 300, 300, event_b_ms, event_b_ms)
        else:
            continue
        (
            count,
            average,
            precipitation_max,
            precipitation_rate_max,
            wind_speed_max,
            wind_gust_max,
            visibility_min,
            interval_min,
            interval_max,
            first_ms,
            last_ms,
        ) = values
        key = (location_id, start_ms, start_ms + _WINDOW_MS)
        expected[key] = _expected_metric(
            location_id=location_id,
            start_ms=start_ms,
            end_ms=start_ms + _WINDOW_MS,
            count=count,
            temperature_avg=average,
            precipitation_max=precipitation_max,
            precipitation_rate_max=precipitation_rate_max,
            wind_speed_max=wind_speed_max,
            wind_gust_max=wind_gust_max,
            visibility_min=visibility_min,
            interval_min=interval_min,
            interval_max=interval_max,
            first_ms=first_ms,
            last_ms=last_ms,
        )
    return expected


def _expected_single_window(
    *, location_id: str, timestamp_ms: int, start_ms: int, values: tuple[float, ...]
) -> dict[str, object]:
    temperature, precipitation, rate, wind_speed, wind_gust, visibility, interval = values
    return _expected_metric(
        location_id=location_id,
        start_ms=start_ms,
        end_ms=start_ms + _WINDOW_MS,
        count=1,
        temperature_avg=temperature,
        precipitation_max=precipitation,
        precipitation_rate_max=rate,
        wind_speed_max=wind_speed,
        wind_gust_max=wind_gust,
        visibility_min=visibility,
        interval_min=int(interval),
        interval_max=int(interval),
        first_ms=timestamp_ms,
        last_ms=timestamp_ms,
    )


def _assert_no_active_jobs() -> None:
    active_jobs = [job for job in _jobs_overview() if job.get("state") in _ACTIVE_JOB_STATES]
    if active_jobs:
        names = ", ".join(str(job.get("name", "unknown")) for job in active_jobs)
        pytest.skip(f"active Flink job(s) require coordination before the one-slot test: {names}")


def _wait_for_new_active_qa_job_id(
    job_name: str, excluded_job_ids: set[str], deadline_seconds: float = 10
) -> str | None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        candidates = [
            str(job.get("jid") or job.get("id"))
            for job in _jobs_overview()
            if job.get("name") == job_name
            and job.get("state") in _ACTIVE_JOB_STATES
            and (job.get("jid") or job.get("id"))
            and str(job.get("jid") or job.get("id")) not in excluded_job_ids
        ]
        if len(candidates) == 1:
            return candidates[0]
        time.sleep(0.2)
    return None


def _submit_unbounded_qa_job(
    job_name: str,
    group_id: str,
    *,
    submission: dict[str, str | None],
    checkpoint_interval_seconds: int | None = None,
    restore_metadata: str | None = None,
    business_sinks: bool = False,
    python_entrypoint: str | None = None,
) -> str:
    if business_sinks or python_entrypoint is not None:
        suffix = job_name.removeprefix("weather-flink-qa-")
        if (
            not job_name.startswith("weather-flink-qa-")
            or re.fullmatch(r"[A-Za-z0-9]+", suffix) is None
            or group_id not in {job_name, f"{job_name}-group"}
        ):
            raise ValueError(
                "business QA requires job-name weather-flink-qa-<id> and a matching group-id"
            )
    if python_entrypoint is not None:
        if not business_sinks:
            raise ValueError("alternate QA entrypoint requires business sinks")
        if _QA_ENTRYPOINT_PATTERN.fullmatch(python_entrypoint) is None:
            raise ValueError(
                "alternate QA entrypoint must be a .py file under /tmp/weather-flink-qa-"
            )

    existing_job_ids = set()
    if restore_metadata is not None:
        existing_job_ids = {
            str(job.get("jid") or job.get("id"))
            for job in _jobs_overview()
            if job.get("name") == job_name and (job.get("jid") or job.get("id"))
        }
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "-u",
        "flink",
        "flink-jobmanager",
        "/opt/flink/bin/flink",
        "run",
        "-d",
    ]
    if restore_metadata is not None:
        command.extend(["-s", restore_metadata])
    command.extend(
        [
            "-py",
            python_entrypoint or "/opt/climate/src/climate_logistics/flink_job.py",
            "--job-name",
            job_name,
            "--group-id",
            group_id,
            "--qa-audit",
        ]
    )
    if business_sinks:
        command.append("--qa-business-sinks")
    if checkpoint_interval_seconds is not None:
        command.extend(
            ["--qa-checkpoint-interval-seconds", str(checkpoint_interval_seconds)]
        )

    try:
        result = subprocess.run(
            command,
            cwd=_REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        stdout = getattr(exc, "stdout", None) or getattr(exc, "output", None) or ""
        stderr = getattr(exc, "stderr", None) or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        match = _JOB_ID_PATTERN.search(f"{stdout}\n{stderr}")
        job_id = match.group(1) if match else None
        if job_id in existing_job_ids:
            job_id = None
        if job_id is None and restore_metadata is not None:
            job_id = _wait_for_new_active_qa_job_id(job_name, existing_job_ids)
        if job_id is not None:
            submission["job_id"] = job_id
        raise
    match = _JOB_ID_PATTERN.search(result.stdout + "\n" + result.stderr)
    job_id = match.group(1) if match else None
    if job_id in existing_job_ids:
        job_id = None
    if job_id is None:
        job_id = _wait_for_new_active_qa_job_id(job_name, existing_job_ids)
    if job_id is None:
        raise AssertionError(f"Flink did not expose the submitted QA job {job_name}")
    submission["job_id"] = job_id
    if result.returncode != 0:
        output = result.stdout + "\n" + result.stderr
        raise AssertionError(
            f"Flink CLI failed while submitting this QA job: {output[-3000:]}"
        )

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = _rest_json(f"/jobs/{job_id}").get("state")
        if state == "RUNNING":
            return job_id
        if state in {"FAILED", "CANCELED", "FINISHED"}:
            raise AssertionError(f"unbounded QA job ended {state} during startup")
        time.sleep(0.25)
    raise AssertionError(f"unbounded QA job {job_name} did not reach RUNNING")


def _wait_for_completed_checkpoint(
    job_id: str,
    *,
    after_timestamp_ms: int,
    after_checkpoint_id: int | None = None,
    deadline_seconds: float = 25,
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last_report: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_report = _rest_json(f"/jobs/{job_id}/checkpoints")
        checkpoint = last_report.get("latest", {}).get("completed")
        if checkpoint is not None:
            checkpoint_id = int(checkpoint["id"])
            trigger_timestamp_ms = int(checkpoint["trigger_timestamp"])
            if (
                trigger_timestamp_ms >= after_timestamp_ms
                and (after_checkpoint_id is None or checkpoint_id != after_checkpoint_id)
            ):
                assert int(last_report.get("counts", {}).get("completed", 0)) >= 1
                return checkpoint
        time.sleep(0.2)
    raise AssertionError(
        "no completed checkpoint triggered after the required point; "
        f"job={job_id}, report={last_report}"
    )


def _trigger_checkpoint(job_id: str) -> dict[str, Any]:
    request = Request(
        f"{_REST_URL}/jobs/{job_id}/checkpoints",
        data=json.dumps({"checkpointType": "CONFIGURED"}).encode("utf-8"),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            result = json.loads(response.read())
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        message = details.lower()
        if "checkpoint" in message and any(
            phrase in message for phrase in ("already", "in progress", "pending")
        ):
            return {"already_in_progress": True}
        raise AssertionError(f"Flink rejected a checkpoint trigger: {details}") from exc
    if not result.get("request-id") and not result.get("requestId"):
        raise AssertionError(f"Flink did not acknowledge the checkpoint trigger: {result}")
    return result


def _cancel_own_qa_job_via_rest(job_id: str, job_name: str) -> None:
    job = _rest_json(f"/jobs/{job_id}")
    if job.get("name") != job_name:
        raise AssertionError(f"refusing to cancel unexpected job id {job_id}")
    if job.get("state") not in _ACTIVE_JOB_STATES:
        return
    request = Request(
        f"{_REST_URL}/jobs/{job_id}?mode=cancel",
        headers={"Accept": "application/json"},
        method="PATCH",
    )
    with urlopen(request, timeout=5) as response:
        if response.status != 202:
            raise AssertionError(f"Flink did not accept cancellation for {job_id}")


def _wait_for_restored_checkpoint(
    job_id: str, expected_checkpoint_id: int, deadline_seconds: float = 30
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last_report: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_report = _rest_json(f"/jobs/{job_id}/checkpoints")
        restored = last_report.get("latest", {}).get("restored")
        if restored is not None and int(restored.get("id", -1)) == expected_checkpoint_id:
            return restored
        time.sleep(0.2)
    raise AssertionError(
        f"job {job_id} did not report restored checkpoint {expected_checkpoint_id}: "
        f"{last_report}"
    )


def _checkpoint_metadata_path(checkpoint: dict[str, Any]) -> str:
    external_path = str(checkpoint["external_path"])
    parsed = urlparse(external_path)
    if parsed.scheme != "file":
        raise AssertionError(f"checkpoint must use shared file storage: {external_path}")
    checkpoint_path = unquote(parsed.path).rstrip("/")
    return f"{checkpoint_path}/_metadata"


def _assert_checkpoint_readable_on_both_flink_nodes(metadata_path: str) -> None:
    for service in ("flink-jobmanager", "flink-taskmanager"):
        subprocess.run(
            ["docker", "compose", "exec", "-T", service, "test", "-r", metadata_path],
            cwd=_REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )


def _assert_jobmanager_retains_two_checkpoints() -> None:
    configuration = _rest_json("/jobmanager/config")
    if isinstance(configuration, list):
        entries = configuration
    elif isinstance(configuration, dict):
        entries = configuration.get("config", [])
    else:
        raise AssertionError(f"unexpected JobManager config response: {configuration}")
    values = {
        str(entry.get("key")): str(entry.get("value"))
        for entry in entries
        if isinstance(entry, dict)
    }
    assert values.get("execution.checkpointing.num-retained") == "2"


def _cleanup_unbounded_qa_job(
    job_name: str,
    job_id: str | None,
    submission: dict[str, str | None],
) -> None:
    known_job_id = job_id or submission.get("job_id")
    own_job_id = _wait_for_own_job_id(job_name, known_job_id)
    if own_job_id:
        job = _rest_json(f"/jobs/{own_job_id}")
        if job.get("name") != job_name:
            raise AssertionError(f"refusing to clean up unexpected job {own_job_id}")
        if job.get("state") in _ACTIVE_JOB_STATES:
            _cancel_own_active_job(own_job_id)
        _wait_for_qa_job_terminal(own_job_id, job_name)


def _wait_for_qa_job_terminal(
    job_id: str,
    job_name: str,
    deadline_seconds: float = 30,
) -> str:
    deadline = time.monotonic() + deadline_seconds
    last_state: str | None = None
    while time.monotonic() < deadline:
        job = _rest_json(f"/jobs/{job_id}")
        if job.get("name") != job_name:
            raise AssertionError(f"refusing to wait for unexpected job {job_id}")
        last_state = str(job.get("state"))
        if last_state in _TERMINAL_JOB_STATES:
            return last_state
        time.sleep(0.2)
    raise AssertionError(
        f"QA Flink job {job_name} did not reach a terminal state: {last_state}"
    )


def _vertex_id(job_id: str, name_fragment: str) -> str:
    vertices = _rest_json(f"/jobs/{job_id}").get("vertices", [])
    matches = [
        vertex
        for vertex in vertices
        if name_fragment in str(vertex.get("name", ""))
    ]
    if len(matches) != 1:
        names = [str(vertex.get("name", "")) for vertex in vertices]
        raise AssertionError(f"expected one {name_fragment} vertex, found {names}")
    return str(matches[0]["id"])


def _subtask_metric_value(job_id: str, vertex_id: str, suffix: str) -> int:
    endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/0/metrics"
    metrics = _rest_json(endpoint)
    matches = [
        str(metric["id"])
        for metric in metrics
        if suffix in str(metric.get("id", ""))
    ]
    if not matches:
        raise AssertionError(f"metric {suffix} is not registered on vertex {vertex_id}")
    metric_id = matches[0]
    requested = _rest_json(f"{endpoint}?get={quote(metric_id, safe='')}")
    if not requested or requested[0].get("value") is None:
        raise AssertionError(f"metric {metric_id} has no value")
    return int(requested[0]["value"])


def _current_watermark(job_id: str) -> int:
    vertex_id = _vertex_id(job_id, "weather-temporal-windows")
    try:
        return _subtask_metric_value(job_id, vertex_id, "currentInputWatermark")
    except AssertionError:
        return _subtask_metric_value(job_id, vertex_id, "currentOutputWatermark")


def _wait_for_watermark(job_id: str, target_ms: int, deadline_seconds: float = 30) -> int:
    deadline = time.monotonic() + deadline_seconds
    last_seen: int | None = None
    while time.monotonic() < deadline:
        last_seen = _current_watermark(job_id)
        if last_seen >= target_ms:
            return last_seen
        time.sleep(0.25)
    raise AssertionError(f"watermark stayed below {target_ms}; last observed {last_seen}")


def _source_offsets(job_id: str) -> dict[str, int]:
    vertex_id = _vertex_id(job_id, "weather-raw-source")
    endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/0/metrics"
    metrics = _rest_json(endpoint)
    offset_ids = [
        str(metric["id"])
        for metric in metrics
        if "currentOffset" in str(metric.get("id", ""))
        and "committed" not in str(metric.get("id", "")).lower()
    ]
    if not offset_ids:
        raise AssertionError("Kafka source has no currentOffset metrics for its partitions")
    requested = _rest_json(
        f"{endpoint}?get={quote(','.join(offset_ids), safe=',')}"
    )
    return {str(metric["id"]): int(metric["value"]) for metric in requested}


def _wait_for_source_offsets(job_id: str, expected_partitions: int = 3) -> dict[str, int]:
    deadline = time.monotonic() + 30
    last_seen: dict[str, int] = {}
    while time.monotonic() < deadline:
        try:
            last_seen = _source_offsets(job_id)
        except AssertionError:
            time.sleep(0.25)
            continue
        if len(last_seen) >= expected_partitions:
            return last_seen
        time.sleep(0.25)
    raise AssertionError(f"Kafka source offsets were not available: {last_seen}")


def _wait_for_offset_advance(job_id: str, before: dict[str, int]) -> dict[str, int]:
    deadline = time.monotonic() + 30
    last_seen = before
    while time.monotonic() < deadline:
        last_seen = _source_offsets(job_id)
        if any(value > before.get(metric_id, -1) for metric_id, value in last_seen.items()):
            return last_seen
        time.sleep(0.25)
    raise AssertionError(f"Kafka source offsets did not advance after publishing: {last_seen}")


def _wait_for_metric_keys(
    started_at: str,
    location_ids: set[str],
    expected_keys: set[tuple[str, int, int]],
    deadline_seconds: float = 30,
) -> tuple[str, list[dict[str, Any]]]:
    deadline = time.monotonic() + deadline_seconds
    last_logs = ""
    last_records: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_logs = _taskmanager_logs_since(started_at)
        last_records = _metric_records(last_logs, location_ids)
        if expected_keys.issubset({_metric_key(record) for record in last_records}):
            return last_logs, last_records
        time.sleep(0.25)
    raise AssertionError(
        "window outputs did not reach "
        f"{expected_keys}; observed {_metric_records(last_logs, location_ids)}"
    )


def _late_records_count(job_id: str) -> int:
    vertex_id = _vertex_id(job_id, "weather-late-counter")
    return _subtask_metric_value(job_id, vertex_id, "late_records")


def _wait_for_late_count(job_id: str, expected: int, deadline_seconds: float = 30) -> int:
    deadline = time.monotonic() + deadline_seconds
    last_seen: int | None = None
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            last_seen = _late_records_count(job_id)
        except AssertionError as exc:
            last_error = str(exc)
            if "is not registered" not in last_error and "has no value" not in last_error:
                raise
        else:
            last_error = None
            if last_seen == expected:
                return last_seen
        time.sleep(0.25)
    raise AssertionError(
        f"late_records did not reach {expected}; observed {last_seen}; "
        f"last metric error: {last_error}"
    )


@pytest.mark.integration
def test_real_flink_bounded_temporal_metrics_match_six_window_oracle():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")
    _assert_no_active_jobs()

    suffix = uuid.uuid4().hex[:12]
    location_a = f"QA-TEMP-A-{suffix}"
    location_c = f"QA-TEMP-C-{suffix}"
    now = datetime.now(UTC)
    now_ms = _epoch_millis(now)
    base_ms = now_ms // _SLIDE_MS * _SLIDE_MS
    base = _EPOCH + timedelta(milliseconds=base_ms)
    timestamp_a = base + timedelta(seconds=90)
    timestamp_b = base + timedelta(seconds=30)
    timestamp_c = base + timedelta(milliseconds=_SLIDE_MS)
    original_ingestion = now
    revised_ingestion = now + timedelta(seconds=1)
    records = [
        _publish_weather(
            location_a,
            timestamp_a,
            original_ingestion,
            temperature=20.0,
            precipitation=2.0,
            interval=900,
            wind_speed=40.0,
            wind_gust=55.0,
            visibility=9_000.0,
        ),
        _publish_weather(
            location_a,
            timestamp_b,
            original_ingestion,
            temperature=30.0,
            precipitation=1.0,
            interval=300,
            wind_speed=50.0,
            wind_gust=60.0,
            visibility=6_000.0,
        ),
        _publish_weather(
            location_a,
            timestamp_a,
            revised_ingestion,
            temperature=99.0,
            precipitation=999.0,
            interval=1,
            wind_speed=99.0,
            wind_gust=100.0,
            visibility=1.0,
        ),
        _publish_weather(
            location_c,
            timestamp_c,
            original_ingestion,
            temperature=15.0,
            precipitation=0.5,
            interval=600,
            wind_speed=20.0,
            wind_gust=25.0,
            visibility=10_000.0,
        ),
    ]
    job_name = f"weather-flink-qa-{suffix}"
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None

    starts_ab = [
        base_ms + offset
        for offset in (-1_500_000, -1_200_000, -900_000, -600_000, -300_000, 0)
    ]
    expected = _expected_pair_windows(
        location_id=location_a,
        event_a_ms=base_ms + 90_000,
        event_b_ms=base_ms + 30_000,
        starts_ms=starts_ab,
    )
    starts_c = [
        base_ms + offset
        for offset in (-1_200_000, -900_000, -600_000, -300_000, 0, 300_000)
    ]
    event_c_ms = base_ms + _SLIDE_MS
    for start_ms in starts_c:
        key = (location_c, start_ms, start_ms + _WINDOW_MS)
        expected[key] = _expected_single_window(
            location_id=location_c,
            timestamp_ms=event_c_ms,
            start_ms=start_ms,
            values=(15.0, 0.5, 3.0, 20.0, 25.0, 10_000.0, 600.0),
        )

    try:
        _publish_temporal_records(records)
        job_id, state, _ = _wait_for_bounded_job(
            job_name,
            submission,
            qa_audit=True,
        )
        assert state == "FINISHED"

        logs = _taskmanager_logs_since(started_at)
        actual = _metric_records(logs, {location_a, location_c})
        _assert_metric_results(actual, expected)

        audit_counts = Counter(
            (
                record.get("location_id"),
                record.get("event_timestamp"),
                record.get("status"),
            )
            for _, record in _structured_log_entries(logs)
            if record.get("component") == "flink"
            and record.get("operation") == "data_quality"
            and record.get("location_id") in {location_a, location_c}
        )
        assert audit_counts == Counter(
            [
                (location_a, _timestamp_text(timestamp_a), "accepted"),
                (location_a, _timestamp_text(timestamp_b), "accepted"),
                (location_a, _timestamp_text(timestamp_a), "duplicate"),
                (location_c, _timestamp_text(timestamp_c), "accepted"),
            ]
        )
    finally:
        job_id = _wait_for_own_job_id(job_name, job_id or submission.get("job_id"))
        if job_id:
            _cancel_own_active_job(job_id)


@pytest.mark.integration
def test_real_flink_unbounded_partial_and_fully_late_event_time_behavior():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")
    _assert_no_active_jobs()

    running_services = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--services"],
        cwd=_REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    ).stdout.split()
    if "producer" in running_services:
        pytest.skip("pause the known Producer before testing historical unbounded watermarks")

    suffix = uuid.uuid4().hex[:12]
    location = f"QA-TEMP-UNBOUNDED-{suffix}"
    advance_one_location = f"QA-TEMP-ADV1-{suffix}"
    advance_two_location = f"QA-TEMP-ADV2-{suffix}"
    now = datetime.now(UTC)
    now_ms = _epoch_millis(now)
    event_a_ms = now_ms // _SLIDE_MS * _SLIDE_MS - 80 * 60_000
    event_a = _EPOCH + timedelta(milliseconds=event_a_ms)
    event_b = event_a - timedelta(minutes=1)
    event_c = event_a + timedelta(minutes=5)
    advance_one = event_a + timedelta(minutes=33)
    advance_two = event_a + timedelta(minutes=70)
    fully_late = event_a - timedelta(seconds=30)
    ingestion = now
    job_name = f"weather-flink-qa-{suffix}"
    group_id = f"{job_name}-group"
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    job_id: str | None = None
    submission: dict[str, str | None] = {"job_id": None}

    starts_ab = [
        event_a_ms + offset
        for offset in (-1_800_000, -1_500_000, -1_200_000, -900_000, -600_000, -300_000, 0)
    ]
    expected_ab = _expected_pair_windows(
        location_id=location,
        event_a_ms=event_a_ms,
        event_b_ms=event_a_ms - 60_000,
        starts_ms=starts_ab,
    )
    c_window_start = event_a_ms + 300_000
    c_expected = _expected_single_window(
        location_id=location,
        timestamp_ms=event_a_ms + 300_000,
        start_ms=c_window_start,
        values=(15.0, 0.5, 3.0, 20.0, 25.0, 10_000.0, 600.0),
    )
    c_key = (location, c_window_start, c_window_start + _WINDOW_MS)
    expected_all = {**expected_ab, c_key: c_expected}

    try:
        job_id = _submit_unbounded_qa_job(job_name, group_id, submission=submission)
        source_offsets_before = _wait_for_source_offsets(job_id)

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    event_a,
                    ingestion,
                    temperature=20.0,
                    precipitation=2.0,
                    interval=900,
                    wind_speed=40.0,
                    wind_gust=55.0,
                    visibility=9_000.0,
                ),
                _publish_weather(
                    location,
                    event_b,
                    ingestion,
                    temperature=30.0,
                    precipitation=1.0,
                    interval=300,
                    wind_speed=50.0,
                    wind_gust=60.0,
                    visibility=6_000.0,
                ),
            ]
        )
        _wait_for_qa_audit_count(
            started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_a),
            status="accepted",
            count=1,
        )
        accepted_b, last_accept_time = _wait_for_qa_audit_count(
            started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_b),
            status="accepted",
            count=1,
        )
        assert accepted_b
        source_offsets_after = _wait_for_offset_advance(job_id, source_offsets_before)
        assert any(
            value > source_offsets_before.get(metric_id, -1)
            for metric_id, value in source_offsets_after.items()
        )
        _wait_for_watermark(job_id, event_a_ms - _OUT_OF_ORDER_MS - 1)

        idle_deadline = last_accept_time + 65.0
        while time.monotonic() < idle_deadline:
            logs = _taskmanager_logs_since(started_at)
            assert not _metric_records(logs, {location}), (
                "wall-clock idleness must not close event-time windows"
            )
            time.sleep(0.25)
        idle_watermark = _current_watermark(job_id)
        assert idle_watermark <= event_a_ms - _OUT_OF_ORDER_MS

        _publish_temporal_records(
            [
                _publish_weather(
                    advance_one_location,
                    advance_one,
                    ingestion,
                    temperature=10.0,
                    precipitation=0.0,
                    interval=900,
                    wind_speed=5.0,
                    wind_gust=8.0,
                    visibility=10_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            started_at,
            location_id=advance_one_location,
            event_timestamp=_timestamp_text(advance_one),
            status="accepted",
            count=1,
        )
        _wait_for_watermark(job_id, event_a_ms + 31 * 60_000 - 1)
        logs, metric_rows = _wait_for_metric_keys(
            started_at,
            {location},
            set(expected_ab),
        )
        _assert_metric_results(_metric_records(logs, {location}), expected_ab)

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    event_c,
                    ingestion,
                    temperature=15.0,
                    precipitation=0.5,
                    interval=600,
                    wind_speed=20.0,
                    wind_gust=25.0,
                    visibility=10_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_c),
            status="accepted",
            count=1,
        )
        assert {_metric_key(record) for record in metric_rows} == set(expected_ab)

        _publish_temporal_records(
            [
                _publish_weather(
                    advance_two_location,
                    advance_two,
                    ingestion,
                    temperature=11.0,
                    precipitation=0.0,
                    interval=900,
                    wind_speed=6.0,
                    wind_gust=9.0,
                    visibility=10_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            started_at,
            location_id=advance_two_location,
            event_timestamp=_timestamp_text(advance_two),
            status="accepted",
            count=1,
        )
        _wait_for_watermark(job_id, event_a_ms + 68 * 60_000 - 1)
        logs, _ = _wait_for_metric_keys(
            started_at,
            {location},
            set(expected_all),
        )
        _assert_metric_results(_metric_records(logs, {location}), expected_all)
        metrics_before_fully_late = Counter(
            json.dumps(record, sort_keys=True)
            for record in _metric_records(logs, {location})
        )

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    fully_late,
                    ingestion,
                    temperature=99.0,
                    precipitation=999.0,
                    interval=1,
                    wind_speed=99.0,
                    wind_gust=100.0,
                    visibility=1.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            started_at,
            location_id=location,
            event_timestamp=_timestamp_text(fully_late),
            status="accepted",
            count=1,
        )
        assert _wait_for_late_count(job_id, 1) == 1

        final_logs = _taskmanager_logs_since(started_at)
        final_records = _metric_records(final_logs, {location})
        assert Counter(json.dumps(record, sort_keys=True) for record in final_records) == (
            metrics_before_fully_late
        ), "fully late event must not reopen or revise closed windows"
    finally:
        _cleanup_unbounded_qa_job(job_name, job_id, submission)


@pytest.mark.integration
def test_real_flink_restore_replays_post_checkpoint_offset_and_preserves_state():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")
    _assert_no_active_jobs()

    running_services = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--services"],
        cwd=_REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    ).stdout.split()
    if "producer" in running_services:
        pytest.skip("pause the known Producer before testing historical unbounded watermarks")
    _assert_jobmanager_retains_two_checkpoints()

    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-{suffix}"
    group_id = f"{job_name}-group"
    location = f"QA-CHECKPOINT-{suffix}"
    sentinel_location = f"QA-CHECKPOINT-SENTINEL-{suffix}"
    event_a = datetime(2020, 1, 1, 12, 10, tzinfo=UTC)
    event_b = event_a - timedelta(minutes=1)
    sentinel = event_a + timedelta(minutes=40)
    event_a_ms = _epoch_millis(event_a)
    event_b_ms = _epoch_millis(event_b)
    sentinel_ms = _epoch_millis(sentinel)
    job_started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    initial_submission: dict[str, str | None] = {"job_id": None}
    restored_submission: dict[str, str | None] = {"job_id": None}
    initial_job_id: str | None = None
    restored_job_id: str | None = None
    checkpoint_id: int | None = None

    try:
        initial_job_id = _submit_unbounded_qa_job(
            job_name,
            group_id,
            submission=initial_submission,
            checkpoint_interval_seconds=10,
        )
        _wait_for_source_offsets(initial_job_id)

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    event_a,
                    event_a,
                    temperature=20.0,
                    precipitation=2.0,
                    interval=900,
                    wind_speed=40.0,
                    wind_gust=55.0,
                    visibility=9_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            job_started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_a),
            status="accepted",
            count=1,
        )
        accepted_a_at_ms = _epoch_millis(datetime.now(UTC))
        checkpoint_c = _wait_for_completed_checkpoint(
            initial_job_id,
            after_timestamp_ms=accepted_a_at_ms,
            deadline_seconds=25,
        )
        checkpoint_id = int(checkpoint_c["id"])
        checkpoint_captured_at = time.monotonic()
        metadata_path = _checkpoint_metadata_path(checkpoint_c)
        _assert_checkpoint_readable_on_both_flink_nodes(metadata_path)
        source_offsets_at_checkpoint = _source_offsets(initial_job_id)

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    event_b,
                    event_b,
                    temperature=30.0,
                    precipitation=1.0,
                    interval=300,
                    wind_speed=50.0,
                    wind_gust=60.0,
                    visibility=6_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            job_started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_b),
            status="accepted",
            count=1,
        )
        offsets_after_b = _wait_for_offset_advance(
            initial_job_id, source_offsets_at_checkpoint
        )
        assert any(
            offsets_after_b.get(metric_id, -1) > offset
            for metric_id, offset in source_offsets_at_checkpoint.items()
        )
        accepted_b_at_ms = _epoch_millis(datetime.now(UTC))
        _trigger_checkpoint(initial_job_id)
        checkpoint_after_b = _wait_for_completed_checkpoint(
            initial_job_id,
            after_timestamp_ms=accepted_b_at_ms,
            after_checkpoint_id=checkpoint_id,
            deadline_seconds=17,
        )
        assert int(checkpoint_after_b["id"]) != checkpoint_id
        assert time.monotonic() - checkpoint_captured_at < 19

        _cancel_own_qa_job_via_rest(initial_job_id, job_name)
        cancellation_deadline = time.monotonic() + 10
        while time.monotonic() < cancellation_deadline:
            if _rest_json(f"/jobs/{initial_job_id}").get("state") == "CANCELED":
                break
            time.sleep(0.2)
        assert _rest_json(f"/jobs/{initial_job_id}").get("state") == "CANCELED"
        assert time.monotonic() - checkpoint_captured_at < 20
        _assert_checkpoint_readable_on_both_flink_nodes(metadata_path)

        restored_started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        restored_started_at_ms = _epoch_millis(datetime.now(UTC))
        restored_job_id = _submit_unbounded_qa_job(
            job_name,
            group_id,
            submission=restored_submission,
            checkpoint_interval_seconds=10,
            restore_metadata=metadata_path,
        )
        assert restored_job_id != initial_job_id
        restored_checkpoint = _wait_for_restored_checkpoint(
            restored_job_id, checkpoint_id
        )
        assert int(restored_checkpoint["id"]) == checkpoint_id

        _wait_for_qa_audit_count(
            restored_started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_b),
            status="accepted",
            count=1,
        )
        restored_offsets = _wait_for_source_offsets(restored_job_id)
        assert any(
            restored_offsets.get(metric_id, -1) > offset
            for metric_id, offset in source_offsets_at_checkpoint.items()
        )

        _publish_temporal_records(
            [
                _publish_weather(
                    location,
                    event_a,
                    event_a + timedelta(seconds=30),
                    temperature=99.0,
                    precipitation=999.0,
                    interval=1,
                    wind_speed=99.0,
                    wind_gust=100.0,
                    visibility=1.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            restored_started_at,
            location_id=location,
            event_timestamp=_timestamp_text(event_a),
            status="duplicate",
            count=1,
        )

        _publish_temporal_records(
            [
                _publish_weather(
                    sentinel_location,
                    sentinel,
                    sentinel,
                    temperature=10.0,
                    precipitation=0.0,
                    interval=900,
                    wind_speed=5.0,
                    wind_gust=8.0,
                    visibility=10_000.0,
                )
            ]
        )
        _wait_for_qa_audit_count(
            restored_started_at,
            location_id=sentinel_location,
            event_timestamp=_timestamp_text(sentinel),
            status="accepted",
            count=1,
        )
        _wait_for_watermark(restored_job_id, sentinel_ms - _OUT_OF_ORDER_MS - 1)

        starts_ab = list(
            range(
                event_a_ms - _WINDOW_MS,
                event_a_ms + 1,
                _SLIDE_MS,
            )
        )
        expected = _expected_pair_windows(
            location_id=location,
            event_a_ms=event_a_ms,
            event_b_ms=event_b_ms,
            starts_ms=starts_ab,
        )
        logs, _ = _wait_for_metric_keys(
            restored_started_at,
            {location},
            set(expected),
        )
        _assert_metric_results(_metric_records(logs, {location}), expected)
        assert any(metric["event_count"] == 2 for metric in expected.values())

        _trigger_checkpoint(restored_job_id)
        checkpoint_after_restore = _wait_for_completed_checkpoint(
            restored_job_id,
            after_timestamp_ms=restored_started_at_ms,
            deadline_seconds=30,
        )
        assert int(checkpoint_after_restore["trigger_timestamp"]) >= restored_started_at_ms
    finally:
        known_job_ids = {
            str(job_id)
            for job_id in (
                initial_job_id,
                restored_job_id,
                initial_submission.get("job_id"),
                restored_submission.get("job_id"),
            )
            if job_id
        }
        if initial_job_id is None and initial_submission.get("job_id") is None:
            fallback_initial_id = _wait_for_new_active_qa_job_id(job_name, set())
            if fallback_initial_id is not None:
                known_job_ids.add(fallback_initial_id)
        if restored_job_id is None and restored_submission.get("job_id") is None:
            fallback_restored_id = _wait_for_new_active_qa_job_id(
                job_name, known_job_ids
            )
            if fallback_restored_id is not None:
                known_job_ids.add(fallback_restored_id)
        for own_job_id in known_job_ids:
            _cancel_own_active_job(own_job_id)
