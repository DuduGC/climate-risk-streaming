from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SLIDE_MS = 300_000
_WINDOW_MS = 1_800_000
_OUT_OF_ORDER_MS = 120_000
_ACTIVE_JOB_STATES = {"CREATED", "INITIALIZING", "RESTARTING", "RUNNING", "CANCELLING"}


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


def _submit_unbounded_qa_job(
    job_name: str,
    group_id: str,
    *,
    submission: dict[str, str | None],
) -> str:
    result = subprocess.run(
        [
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
            "-py",
            "/opt/climate/src/climate_logistics/flink_job.py",
            "--job-name",
            job_name,
            "--group-id",
            group_id,
            "--qa-audit",
        ],
        cwd=_REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
    )
    match = _JOB_ID_PATTERN.search(result.stdout + "\n" + result.stderr)
    job_id = match.group(1) if match else _wait_for_own_job_id(job_name, None)
    if job_id is None:
        raise AssertionError(f"Flink did not expose the submitted QA job {job_name}")
    submission["job_id"] = job_id

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = _rest_json(f"/jobs/{job_id}").get("state")
        if state == "RUNNING":
            return job_id
        if state in {"FAILED", "CANCELED", "FINISHED"}:
            raise AssertionError(f"unbounded QA job ended {state} during startup")
        time.sleep(0.25)
    raise AssertionError(f"unbounded QA job {job_name} did not reach RUNNING")


def _cleanup_unbounded_qa_job(
    job_name: str,
    job_id: str | None,
    submission: dict[str, str | None],
) -> None:
    known_job_id = job_id or submission.get("job_id")
    own_job_id = _wait_for_own_job_id(job_name, known_job_id)
    if own_job_id:
        _cancel_own_active_job(own_job_id)


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
