from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import uuid
from datetime import UTC, datetime
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

if producer.flush(20) or delivery_errors:
    raise SystemExit("Kafka delivery confirmation failed")
print("DELIVERED=2")
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
    records = [
        {"key": location_id, "value": "{invalid-json"},
        {"key": location_id, "value": valid_payload},
    ]
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
    assert "DELIVERED=2" in result.stdout


def _read_capture(capture: BinaryIO) -> str:
    capture.seek(0)
    return capture.read().decode("utf-8", errors="replace")


def _wait_for_bounded_job(job_name: str, submission: dict[str, str | None]) -> tuple[str, str, str]:
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