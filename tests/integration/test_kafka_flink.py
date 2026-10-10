from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

pytestmark = pytest.mark.integration

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_REST_URL = "http://127.0.0.1:8081"
_DEADLINE_SECONDS = 240
_ACTIVE_STATES = {"CREATED", "INITIALIZING", "RESTARTING", "RUNNING", "CANCELLING"}
_TERMINAL_STATES = {"CANCELED", "FAILED", "FINISHED"}
_JOB_ID_PATTERN = re.compile(
    r"(?:JOB_ID=|submitted with JobID\s+)([0-9a-fA-F-]{32,36})"
)

_PUBLISHER_SCRIPT = """\
import json
import sys
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "kafka:9092",
    "enable.idempotence": True,
    "acks": "all",
})
delivery_errors = []
delivered = 0

def on_delivery(error, message):
    if error is not None:
        delivery_errors.append(error)

for line in sys.stdin.buffer:
    record = json.loads(line)
    producer.produce(
        "weather.raw",
        key=record["key"].encode("utf-8"),
        value=record["value"].encode("utf-8"),
        on_delivery=on_delivery,
    )
    delivered += 1

if producer.flush(20) or delivery_errors:
    raise SystemExit("Kafka delivery confirmation failed")
print(f"DELIVERED={delivered}")
"""


def _rest_json(path: str) -> dict[str, Any]:
    request = Request(f"{_REST_URL}{path}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def _jobs_overview() -> list[dict[str, Any]]:
    return _rest_json("/jobs/overview").get("jobs", [])


def _jobs_named(name: str) -> list[dict[str, Any]]:
    return [job for job in _jobs_overview() if job.get("name") == name]


def _job_id_from_overview(name: str) -> str | None:
    matches = _jobs_named(name)
    if len(matches) != 1:
        return None
    return matches[0].get("jid") or matches[0].get("id")


def _wait_for_own_job_id(name: str, submitted_id: str | None) -> str | None:
    if submitted_id:
        return submitted_id
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job_id = _job_id_from_overview(name)
        if job_id:
            return job_id
        time.sleep(0.5)
    return None


def _cancel_own_active_job(job_id: str) -> None:
    try:
        job = _rest_json(f"/jobs/{job_id}")
    except HTTPError as exc:
        if exc.code == 404:
            return
        raise
    if job.get("state") not in _ACTIVE_STATES:
        return
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
            "cancel",
            job_id,
        ],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(f"could not cancel this test's Flink job {job_id}")


def _publish_fixture_pair(location_id: str, valid_payload: str) -> None:
    _publish_records(
        [
            {"key": location_id, "value": "{invalid-json"},
            {"key": location_id, "value": valid_payload},
        ]
    )


def _publish_records(records: list[dict[str, str]]) -> None:
    stdin = "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "producer", "python", "-c", _PUBLISHER_SCRIPT],
        cwd=_REPOSITORY_ROOT,
        input=stdin,
        check=True,
        capture_output=True,
        text=True,
        timeout=40,
    )
    assert f"DELIVERED={len(records)}" in result.stdout


def _read_capture(capture: BinaryIO) -> str:
    capture.seek(0)
    return capture.read().decode("utf-8", errors="replace")


def _wait_for_bounded_job(
    job_name: str,
    submission: dict[str, str | None],
    *,
    qa_audit: bool = False,
    dedup_ttl_seconds: int | None = None,
) -> tuple[str, str, str]:
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
        "-py",
        "/opt/climate/src/climate_logistics/flink_job.py",
        "--bounded",
        "--job-name",
        job_name,
        "--group-id",
        f"{job_name}-group",
    ]
    if qa_audit:
        command.append("--qa-audit")
    if dedup_ttl_seconds is not None:
        command.extend(["--qa-dedup-ttl-seconds", str(dedup_ttl_seconds)])
    deadline = time.monotonic() + _DEADLINE_SECONDS
    missing_id_deadline: float | None = None
    job_id: str | None = None
    state: str | None = None

    with tempfile.TemporaryFile() as capture:
        process = subprocess.Popen(
            command,
            cwd=_REPOSITORY_ROOT,
            stdout=capture,
            stderr=subprocess.STDOUT,
        )
        try:
            while time.monotonic() < deadline:
                output = _read_capture(capture)
                match = _JOB_ID_PATTERN.search(output)
                if match and job_id is None:
                    job_id = match.group(1)
                    submission["job_id"] = job_id
                    missing_id_deadline = time.monotonic() + 15

                matches = _jobs_named(job_name)
                if matches:
                    job_id = matches[0].get("jid") or matches[0].get("id") or job_id
                    state = matches[0].get("state")
                    submission["job_id"] = job_id

                if job_id:
                    try:
                        state = _rest_json(f"/jobs/{job_id}").get("state") or state
                    except HTTPError as exc:
                        if exc.code != 404:
                            raise

                if state in _TERMINAL_STATES:
                    break
                if process.poll() is not None and process.returncode != 0:
                    output = _read_capture(capture)
                    raise AssertionError(f"Flink CLI failed: {output[-3000:]}")
                if (
                    process.poll() is not None
                    and job_id is None
                    and missing_id_deadline is None
                ):
                    missing_id_deadline = time.monotonic() + 15
                if (
                    missing_id_deadline
                    and time.monotonic() >= missing_id_deadline
                    and job_id is None
                ):
                    output = _read_capture(capture)
                    raise AssertionError(
                        "Flink CLI did not expose the submitted job ID: "
                        f"{output[-2000:]}"
                    )
                time.sleep(0.5)
            else:
                raise AssertionError(f"bounded Flink job {job_name} exceeded {_DEADLINE_SECONDS}s")

            if job_id is None or state is None:
                raise AssertionError(f"Flink did not expose the bounded job {job_name}")
            if state in {"FAILED", "CANCELED"}:
                output = _read_capture(capture)
                raise AssertionError(f"bounded job ended {state}: {output[-3000:]}")
            if state != "FINISHED":
                raise AssertionError(f"bounded job ended {state}, expected FINISHED")
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired as exc:
                raise AssertionError("bounded Flink CLI did not return after FINISHED") from exc
            if process.returncode != 0:
                output = _read_capture(capture)
                raise AssertionError(f"Flink CLI failed after FINISHED: {output[-3000:]}")
            output = _read_capture(capture)
            return job_id, state, output
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def _structured_log_entries(logs: str) -> list[tuple[str, dict[str, Any]]]:
    entries = []
    for line in logs.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            record = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            entries.append((line, record))
    return entries


def _weather_payload(
    location_id: str,
    event_timestamp: datetime,
    ingestion_timestamp: datetime,
    *,
    temperature: float = 27.4,
) -> str:
    return json.dumps(
        {
            "location_id": location_id,
            "city": "Flink QA",
            "latitude": -22.9,
            "longitude": -43.17,
            "temperature": temperature,
            "precipitation": 18.2,
            "wind_speed": 42.1,
            "wind_gust": 58.3,
            "visibility": 7000,
            "weather_code": 61,
            "measurement_interval_seconds": 900,
            "event_timestamp": event_timestamp.isoformat().replace("+00:00", "Z"),
            "ingestion_timestamp": ingestion_timestamp.isoformat().replace("+00:00", "Z"),
            "source": "open-meteo",
            "schema_version": 1,
        },
        separators=(",", ":"),
    )


def _diagnostic(
    location_id: str,
    event_timestamp: datetime,
    ingestion_timestamp: datetime,
) -> dict[str, Any]:
    return {
        "component": "flink",
        "operation": "parse",
        "status": "success",
        "location_id": location_id,
        "event_timestamp": event_timestamp.astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "ingestion_timestamp": ingestion_timestamp.astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "schema_version": 1,
    }


def _taskmanager_logs_since(started_at: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "logs", "--since", started_at, "flink-taskmanager"],
        cwd=_REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout


def _matching_qa_audits(
    logs: str,
    *,
    location_id: str,
    event_timestamp: str,
    status: str,
) -> list[dict[str, Any]]:
    return [
        record
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "data_quality"
        and record.get("location_id") == location_id
        and record.get("event_timestamp") == event_timestamp
        and record.get("status") == status
    ]


def _wait_for_qa_audit_count(
    started_at: str,
    *,
    location_id: str,
    event_timestamp: str,
    status: str,
    count: int,
    deadline_seconds: float = 30,
) -> tuple[list[dict[str, Any]], float]:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        logs = _taskmanager_logs_since(started_at)
        matches = _matching_qa_audits(
            logs,
            location_id=location_id,
            event_timestamp=event_timestamp,
            status=status,
        )
        if len(matches) >= count:
            return matches, time.monotonic()
        time.sleep(0.25)
    raise AssertionError(
        f"QA audit {status} for {location_id}/{event_timestamp} did not reach count {count}"
    )


def _submit_qa_job(
    job_name: str,
    group_id: str,
    *,
    dedup_ttl_seconds: int,
) -> str:
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
        "-py",
        "/opt/climate/src/climate_logistics/flink_job.py",
        "--job-name",
        job_name,
        "--group-id",
        group_id,
        "--qa-dedup-ttl-seconds",
        str(dedup_ttl_seconds),
        "--qa-audit",
    ]
    result = subprocess.run(
        command,
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
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = _rest_json(f"/jobs/{job_id}").get("state")
        if state == "RUNNING":
            return job_id
        if state in _TERMINAL_STATES:
            raise AssertionError(f"QA Flink job ended {state} during startup")
        time.sleep(0.25)
    raise AssertionError(f"QA Flink job {job_name} did not reach RUNNING")


def test_real_flink_discards_invalid_json_then_emits_the_valid_fixture():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")

    active_jobs = [job for job in _jobs_overview() if job.get("state") in _ACTIVE_STATES]
    if active_jobs:
        names = ", ".join(str(job.get("name", "unknown")) for job in active_jobs)
        pytest.skip(f"active Flink job(s) require coordination before the one-slot test: {names}")

    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-{suffix}"
    location_id = f"QA-{suffix}"
    timestamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    valid_payload = json.dumps(
        {
            "location_id": location_id,
            "city": "Flink QA",
            "latitude": -22.9,
            "longitude": -43.17,
            "temperature": 27.4,
            "precipitation": 18.2,
            "wind_speed": 42.1,
            "wind_gust": 58.3,
            "visibility": 7000,
            "weather_code": 61,
            "measurement_interval_seconds": 900,
            "event_timestamp": timestamp,
            "ingestion_timestamp": timestamp,
            "source": "open-meteo",
            "schema_version": 1,
        },
        separators=(",", ":"),
    )
    expected_diagnostic = {
        "component": "flink",
        "operation": "parse",
        "status": "success",
        "location_id": location_id,
        "event_timestamp": timestamp,
        "ingestion_timestamp": timestamp,
        "schema_version": 1,
    }
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None
    try:
        _publish_fixture_pair(location_id, valid_payload)
        job_id, state, _ = _wait_for_bounded_job(job_name, submission)
        assert state == "FINISHED"

        logs = subprocess.run(
            ["docker", "compose", "logs", "--since", started_at, "flink-taskmanager"],
            cwd=_REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        entries = _structured_log_entries(logs)
        assert any(
            "weather-ingestion" in line and record == expected_diagnostic
            for line, record in entries
        ), "TaskManager PrintSink logs lack the exact valid-fixture diagnostic"
        assert any(
            record.get("component") == "flink"
            and record.get("operation") == "parse"
            and record.get("status") == "error"
            and record.get("metric") == "invalid_records"
            for _, record in entries
        ), "TaskManager logs lack controlled invalid-record evidence"
    finally:
        job_id = _wait_for_own_job_id(job_name, job_id or submission.get("job_id"))
        if job_id:
            _cancel_own_active_job(job_id)


def test_real_flink_rejects_bad_times_and_deduplicates_logical_event_identities():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")

    active_jobs = [job for job in _jobs_overview() if job.get("state") in _ACTIVE_STATES]
    if active_jobs:
        names = ", ".join(str(job.get("name", "unknown")) for job in active_jobs)
        pytest.skip(f"active Flink job(s) require coordination before the one-slot test: {names}")

    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-{suffix}"
    location_a = f"QA-A-{suffix}"
    location_b = f"QA-B-{suffix}"
    event_a = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    ingestion_a = event_a + timedelta(seconds=5)
    event_c = event_a + timedelta(seconds=60)
    ingestion_c = event_c + timedelta(seconds=2)
    event_a_key = event_a.isoformat().replace("+00:00", "Z")
    event_c_key = event_c.isoformat().replace("+00:00", "Z")
    revised_offset_a = event_a.astimezone(timezone(timedelta(hours=-3)))
    records = [
        {"key": location_a, "value": "{invalid-json"},
        {
            "key": location_a,
            "value": _weather_payload(
                location_a,
                event_a,
                event_a + timedelta(hours=1),
            ),
        },
        {
            "key": location_a,
            "value": _weather_payload(location_a, event_a, ingestion_a),
        },
        {
            "key": location_a,
            "value": _weather_payload(
                location_a,
                revised_offset_a,
                event_a + timedelta(seconds=30),
                temperature=18.0,
            ),
        },
        {
            "key": location_b,
            "value": _weather_payload(location_b, event_a, ingestion_a),
        },
        {
            "key": location_a,
            "value": _weather_payload(location_a, event_c, ingestion_c),
        },
    ]
    expected_diagnostics = [
        _diagnostic(location_a, event_a, ingestion_a),
        _diagnostic(location_b, event_a, ingestion_a),
        _diagnostic(location_a, event_c, ingestion_c),
    ]
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None
    try:
        _publish_records(records)
        job_id, state, _ = _wait_for_bounded_job(
            job_name,
            submission,
            qa_audit=True,
        )
        assert state == "FINISHED"

        logs = _taskmanager_logs_since(started_at)
        entries = _structured_log_entries(logs)
        diagnostics = [
            record
            for line, record in entries
            if "weather-ingestion" in line
            and record.get("component") == "flink"
            and record.get("operation") == "parse"
            and record.get("status") == "success"
            and record.get("location_id") in {location_a, location_b}
        ]
        assert Counter(json.dumps(record, sort_keys=True) for record in diagnostics) == Counter(
            json.dumps(record, sort_keys=True) for record in expected_diagnostics
        ), "PrintSink output must contain exactly one diagnostic for each valid identity"

        quality_audits = [
            record
            for _, record in entries
            if record.get("component") == "flink"
            and record.get("operation") == "data_quality"
            and record.get("location_id") in {location_a, location_b}
        ]
        expected_audits = Counter(
            [
                (location_a, event_a_key, "rejected", "ingestion_in_future"),
                (location_a, event_a_key, "accepted", None),
                (location_a, event_a_key, "duplicate", "duplicate_identity"),
                (location_b, event_a_key, "accepted", None),
                (location_a, event_c_key, "accepted", None),
            ]
        )
        actual_audits = Counter(
            (
                record.get("location_id"),
                record.get("event_timestamp"),
                record.get("status"),
                record.get("reason"),
            )
            for record in quality_audits
        )
        assert actual_audits == expected_audits, (
            "QA audit must show one rejection, one duplicate and three accepts"
        )
        assert any(
            record.get("operation") == "parse"
            and record.get("status") == "error"
            and record.get("reason") == "weather_parse_error"
            and record.get("metric") == "invalid_records"
            for _, record in entries
        ), "TaskManager logs lack controlled invalid-JSON evidence"
    finally:
        job_id = _wait_for_own_job_id(job_name, job_id or submission.get("job_id"))
        if job_id:
            _cancel_own_active_job(job_id)


def test_real_flink_ttl_expires_on_processing_time_without_duplicate_refresh():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")

    active_jobs = [job for job in _jobs_overview() if job.get("state") in _ACTIVE_STATES]
    if active_jobs:
        names = ", ".join(str(job.get("name", "unknown")) for job in active_jobs)
        pytest.skip(f"active Flink job(s) require coordination before the one-slot test: {names}")

    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-{suffix}"
    group_id = f"{job_name}-group"
    location_id = f"QA-TTL-{suffix}"
    event_timestamp = datetime(2020, 1, 1, 12, tzinfo=UTC)
    ingestion_timestamp = event_timestamp + timedelta(seconds=1)
    event_timestamp_text = event_timestamp.isoformat().replace("+00:00", "Z")
    payload = _weather_payload(location_id, event_timestamp, ingestion_timestamp)
    record = {"key": location_id, "value": payload}
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    job_id: str | None = None
    try:
        job_id = _submit_qa_job(job_name, group_id, dedup_ttl_seconds=20)
        _publish_records([record])
        _, first_accepted_at = _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=event_timestamp_text,
            status="accepted",
            count=1,
        )

        duplicate_publish_at = first_accepted_at + 10.0
        while time.monotonic() < duplicate_publish_at:
            time.sleep(min(0.25, duplicate_publish_at - time.monotonic()))
        _publish_records([record])
        _, duplicate_observed_at = _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=event_timestamp_text,
            status="duplicate",
            count=1,
        )
        duplicate_age = duplicate_observed_at - first_accepted_at
        assert 9.0 <= duplicate_age <= 17.0, (
            "duplicate must be observed near the end of the original 20-second TTL "
            "with at least three seconds of expiry margin; "
            f"observed after {duplicate_age:.2f}s"
        )

        retry_at = first_accepted_at + 23.0
        if retry_at >= duplicate_observed_at + 18.0:
            raise AssertionError("scheduler margin cannot prove no TTL refresh")
        while time.monotonic() < retry_at:
            time.sleep(min(0.25, retry_at - time.monotonic()))
        _publish_records([record])
        _, second_accepted_at = _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=event_timestamp_text,
            status="accepted",
            count=2,
        )

        assert second_accepted_at - first_accepted_at >= 22.0, (
            "the replay must be accepted after the original TTL deadline with margin"
        )
        assert second_accepted_at - duplicate_observed_at < 18.0, (
            "the replay must precede the deadline that a duplicate refresh would create "
            "with at least two seconds of margin"
        )
    finally:
        job_id = _wait_for_own_job_id(job_name, job_id)
        if job_id:
            _cancel_own_active_job(job_id)
