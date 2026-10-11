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

import pytest
from test_flink_temporal import (
    _EPOCH,
    _OUT_OF_ORDER_MS,
    _SLIDE_MS,
    _WINDOW_MS,
    _assert_checkpoint_readable_on_both_flink_nodes,
    _assert_no_active_jobs,
    _epoch_millis,
    _metric_key,
    _publish_temporal_records,
    _publish_weather,
    _submit_unbounded_qa_job,
    _subtask_metric_value,
    _taskmanager_logs_since,
    _timestamp_text,
    _trigger_checkpoint,
    _vertex_id,
    _wait_for_completed_checkpoint,
    _wait_for_metric_keys,
    _wait_for_qa_audit_count,
    _wait_for_restored_checkpoint,
    _wait_for_source_offsets,
    _wait_for_watermark,
)
from test_kafka_flink import (
    _cancel_own_active_job,
    _rest_json,
    _structured_log_entries,
    _wait_for_bounded_job,
    _wait_for_own_job_id,
)

pytestmark = pytest.mark.integration

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_RISK_FIELDS = {
    "risk_score",
    "risk_level",
    "risk_components",
    "risk_rules_version",
}
_METRIC_FIELDS = {
    "component",
    "operation",
    "schema_version",
    "location_id",
    "window_start_ms",
    "window_end_ms",
    "event_count",
    "temperature_avg",
    "precipitation_max",
    "precipitation_rate_max_mm_per_hour",
    "wind_speed_max",
    "wind_gust_max",
    "visibility_min",
    "measurement_interval_min_seconds",
    "measurement_interval_max_seconds",
    "first_event_timestamp_ms",
    "last_event_timestamp_ms",
}


def _canonical(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def _metric_records(logs: str, location_ids: set[str]) -> list[dict[str, Any]]:
    return [
        record
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "window_aggregate"
        and record.get("location_id") in location_ids
    ]


def _risk_records(logs: str, location_ids: set[str]) -> list[dict[str, Any]]:
    return [
        record
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "risk_assessment"
        and record.get("location_id") in location_ids
    ]


def _audit_counts(logs: str, location_ids: set[str]) -> Counter[tuple[str, str, str]]:
    return Counter(
        (
            str(record.get("location_id")),
            str(record.get("event_timestamp")),
            str(record.get("status")),
        )
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "data_quality"
        and record.get("location_id") in location_ids
    )


def _expected_metric(
    *,
    location_id: str,
    timestamp_ms: int,
    start_ms: int,
    precipitation: float,
    precipitation_rate: float,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
) -> dict[str, object]:
    return {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": start_ms,
        "window_end_ms": start_ms + _WINDOW_MS,
        "event_count": 1,
        "temperature_avg": 20.0,
        "precipitation_max": precipitation,
        "precipitation_rate_max_mm_per_hour": precipitation_rate,
        "wind_speed_max": wind_speed,
        "wind_gust_max": wind_gust,
        "visibility_min": visibility,
        "measurement_interval_min_seconds": 900,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": timestamp_ms,
        "last_event_timestamp_ms": timestamp_ms,
    }


def _expected_risk(
    metric: dict[str, object],
    *,
    score: int,
    level: str,
    components: dict[str, int],
) -> dict[str, object]:
    expected = dict(metric)
    expected["operation"] = "risk_assessment"
    expected.update(
        {
            "risk_score": score,
            "risk_level": level,
            "risk_components": components,
            "risk_rules_version": "mvp-1",
        }
    )
    return expected


def _window_starts(timestamp_ms: int) -> list[int]:
    aligned = timestamp_ms // _SLIDE_MS * _SLIDE_MS
    offsets = (-1_500_000, -1_200_000, -900_000, -600_000, -300_000, 0)
    return [aligned + offset for offset in offsets]


def _expected_profile_records(
    *,
    location_id: str,
    timestamp_ms: int,
    precipitation: float,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
    score: int,
    level: str,
    components: dict[str, int],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    metrics: list[dict[str, object]] = []
    assessments: list[dict[str, object]] = []
    for start_ms in _window_starts(timestamp_ms):
        metric = _expected_metric(
            location_id=location_id,
            timestamp_ms=timestamp_ms,
            start_ms=start_ms,
            precipitation=precipitation,
            precipitation_rate=precipitation * 3_600 / 900,
            wind_speed=wind_speed,
            wind_gust=wind_gust,
            visibility=visibility,
        )
        metrics.append(metric)
        assessments.append(
            _expected_risk(
                metric,
                score=score,
                level=level,
                components=components.copy(),
            )
        )
    return metrics, assessments


def _assert_complete_records(
    actual: list[dict[str, Any]], expected: list[dict[str, object]], *, fields: set[str]
) -> None:
    assert len(actual) == len(expected)
    assert all(set(record) == fields for record in actual)
    assert Counter(_canonical(record) for record in actual) == Counter(
        _canonical(record) for record in expected
    )


def _wait_for_assessed_windows(job_id: str, minimum: int, deadline_seconds: float = 30) -> int:
    deadline = time.monotonic() + deadline_seconds
    last_seen: int | None = None
    while time.monotonic() < deadline:
        try:
            vertex_id = _vertex_id(job_id, "weather-risk-assessment")
            last_seen = _subtask_metric_value(job_id, vertex_id, "assessed_windows")
        except AssertionError:
            last_seen = None
        if last_seen is not None and last_seen >= minimum:
            return last_seen
        time.sleep(0.25)
    raise AssertionError(
        f"risk_engine.assessed_windows did not reach {minimum}; last observed {last_seen}"
    )


@pytest.mark.integration
def test_real_flink_bounded_risk_outputs_match_all_four_full_json_contracts():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise Kafka and the Flink cluster")
    _assert_no_active_jobs()

    suffix = uuid.uuid4().hex[:12]
    now = datetime.now(UTC)
    now_ms = _epoch_millis(now)
    aligned_ms = now_ms // _SLIDE_MS * _SLIDE_MS
    timestamp = _EPOCH + timedelta(milliseconds=aligned_ms + 30_000)
    event_ms = _epoch_millis(timestamp)
    ingestion = now
    profiles = {
        "LOW": {
            "precipitation": 0.0,
            "wind_speed": 5.0,
            "wind_gust": 10.0,
            "visibility": 10_000.0,
            "score": 0,
            "components": {
                "precipitation_rate": 0,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        },
        "MEDIUM": {
            "precipitation": 0.5,
            "wind_speed": 5.0,
            "wind_gust": 10.0,
            "visibility": 10_000.0,
            "score": 25,
            "components": {
                "precipitation_rate": 25,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        },
        "HIGH": {
            "precipitation": 0.0,
            "wind_speed": 40.0,
            "wind_gust": 10.0,
            "visibility": 10_000.0,
            "score": 50,
            "components": {
                "precipitation_rate": 0,
                "wind_speed": 50,
                "wind_gust": 0,
                "visibility": 0,
            },
        },
        "CRITICAL": {
            "precipitation": 0.0,
            "wind_speed": 5.0,
            "wind_gust": 10.0,
            "visibility": 199.0,
            "score": 100,
            "components": {
                "precipitation_rate": 0,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 100,
            },
        },
    }
    location_ids = {f"QA-RISK-{level}-{suffix}" for level in profiles}
    records: list[dict[str, str]] = []
    expected_metrics: list[dict[str, object]] = []
    expected_assessments: list[dict[str, object]] = []
    expected_audits: Counter[tuple[str, str, str]] = Counter()

    for level, profile in profiles.items():
        location_id = f"QA-RISK-{level}-{suffix}"
        records.append(
            _publish_weather(
                location_id,
                timestamp,
                ingestion,
                temperature=20.0,
                precipitation=profile["precipitation"],
                interval=900,
                wind_speed=profile["wind_speed"],
                wind_gust=profile["wind_gust"],
                visibility=profile["visibility"],
            )
        )
        if level == "LOW":
            records.append(
                _publish_weather(
                    location_id,
                    timestamp,
                    ingestion + timedelta(seconds=1),
                    temperature=99.0,
                    precipitation=999.0,
                    interval=1,
                    wind_speed=99.0,
                    wind_gust=100.0,
                    visibility=1.0,
                )
            )
            expected_audits[(location_id, _timestamp_text(timestamp), "duplicate")] += 1
        expected_audits[(location_id, _timestamp_text(timestamp), "accepted")] += 1
        metrics, assessments = _expected_profile_records(
            location_id=location_id,
            timestamp_ms=event_ms,
            precipitation=profile["precipitation"],
            wind_speed=profile["wind_speed"],
            wind_gust=profile["wind_gust"],
            visibility=profile["visibility"],
            score=profile["score"],
            level=level,
            components=profile["components"],
        )
        expected_metrics.extend(metrics)
        expected_assessments.extend(assessments)

    job_name = f"weather-flink-qa-{suffix}"
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None
    expected_keys = {
        _metric_key(record)
        for record in expected_metrics
    }

    try:
        _publish_temporal_records(records)
        job_id, state, _ = _wait_for_bounded_job(
            job_name,
            submission,
            qa_audit=True,
        )
        assert state == "FINISHED"

        logs = _taskmanager_logs_since(started_at)
        metric_records = _metric_records(logs, location_ids)
        assert {_metric_key(record) for record in metric_records} == expected_keys
        _assert_complete_records(metric_records, expected_metrics, fields=_METRIC_FIELDS)

        risk_records = _risk_records(logs, location_ids)
        assert {_metric_key(record) for record in risk_records} == expected_keys
        _assert_complete_records(
            risk_records,
            expected_assessments,
            fields=_METRIC_FIELDS | _RISK_FIELDS,
        )
        assert _audit_counts(logs, location_ids) == expected_audits
    finally:
        owned_job_id = _wait_for_own_job_id(job_name, job_id or submission.get("job_id"))
        if owned_job_id:
            _cancel_own_active_job(owned_job_id)


@pytest.mark.integration
def test_real_flink_restores_stage6_checkpoint_and_replays_risk_assessments():
    if os.environ.get("FLINK_INTEGRATION") != "1":
        pytest.skip("set FLINK_INTEGRATION=1 to exercise the Flink checkpoint restore")
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
        pytest.skip("pause the known Producer before replaying the Stage 6 checkpoint")

    manifest_path = _REPOSITORY_ROOT / ".venv" / "stage7-legacy-checkpoint.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["origin_stage"] == 6
    assert manifest["has_risk_branch"] is False
    assert manifest["checkpoint"]["status"] == "COMPLETED"
    assert len(manifest["profiles"]) == 4
    job_name = str(manifest["job_name"])
    group_id = str(manifest["group_id"])
    checkpoint_id = int(manifest["checkpoint"]["id"])
    metadata_path = str(manifest["metadata_path"])
    _assert_checkpoint_readable_on_both_flink_nodes(metadata_path)

    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    restored_started_at_ms = _epoch_millis(datetime.now(UTC))
    location_ids = {str(profile["location_id"]) for profile in manifest["profiles"]}
    expected_metrics: list[dict[str, object]] = []
    expected_assessments: list[dict[str, object]] = []
    expected_keys: set[tuple[str, int, int]] = set()
    duplicate_records: list[dict[str, str]] = []
    profile_levels = {
        "LOW": (
            0,
            {
                "precipitation_rate": 0,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        ),
        "MEDIUM": (
            25,
            {
                "precipitation_rate": 25,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        ),
        "HIGH": (
            50,
            {
                "precipitation_rate": 0,
                "wind_speed": 50,
                "wind_gust": 0,
                "visibility": 0,
            },
        ),
        "CRITICAL": (
            100,
            {
                "precipitation_rate": 0,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 100,
            },
        ),
    }

    for profile in manifest["profiles"]:
        level = str(profile["level"])
        event = json.loads(profile["record"]["value"])
        location_id = str(profile["location_id"])
        assert event["location_id"] == location_id
        assert event["event_timestamp"] == "2020-01-01T12:10:00Z"
        timestamp = datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
        timestamp_ms = _epoch_millis(timestamp)
        score, components = profile_levels[level]
        expected_values = {
            "LOW": (0.0, 0.0, 5.0, 10.0, 10_000.0),
            "MEDIUM": (0.5, 2.0, 5.0, 10.0, 10_000.0),
            "HIGH": (0.0, 0.0, 40.0, 10.0, 10_000.0),
            "CRITICAL": (0.0, 0.0, 5.0, 10.0, 199.0),
        }[level]
        precipitation, precipitation_rate, wind_speed, wind_gust, visibility = expected_values
        assert event["precipitation"] == precipitation
        assert event["wind_speed"] == wind_speed
        assert event["wind_gust"] == wind_gust
        assert event["visibility"] == visibility
        metrics, assessments = _expected_profile_records(
            location_id=location_id,
            timestamp_ms=timestamp_ms,
            precipitation=precipitation,
            wind_speed=wind_speed,
            wind_gust=wind_gust,
            visibility=visibility,
            score=score,
            level=level,
            components=components,
        )
        for metric, assessment in zip(metrics, assessments, strict=True):
            metric["precipitation_rate_max_mm_per_hour"] = precipitation_rate
            assessment["precipitation_rate_max_mm_per_hour"] = precipitation_rate
        expected_metrics.extend(metrics)
        expected_assessments.extend(assessments)
        expected_keys.update(_metric_key(metric) for metric in metrics)

        revised = dict(event)
        revised.update(
            {
                "temperature": 99.0,
                "precipitation": 999.0,
                "wind_speed": 99.0,
                "wind_gust": 100.0,
                "visibility": 1.0,
                "ingestion_timestamp": _timestamp_text(timestamp + timedelta(seconds=30)),
            }
        )
        duplicate_records.append(
            {
                "key": location_id,
                "value": json.dumps(revised, separators=(",", ":")),
            }
        )

    try:
        job_id = _submit_unbounded_qa_job(
            job_name,
            group_id,
            submission=submission,
            checkpoint_interval_seconds=int(manifest["checkpoint_interval_seconds"]),
            restore_metadata=metadata_path,
        )
        assert job_id != manifest["job_id"]
        restored_checkpoint = _wait_for_restored_checkpoint(job_id, checkpoint_id)
        assert int(restored_checkpoint["id"]) == checkpoint_id
        assert _wait_for_source_offsets(job_id)

        _publish_temporal_records(duplicate_records)
        for profile in manifest["profiles"]:
            event_timestamp = str(json.loads(profile["record"]["value"])["event_timestamp"])
            _wait_for_qa_audit_count(
                started_at,
                location_id=str(profile["location_id"]),
                event_timestamp=event_timestamp,
                status="duplicate",
                count=1,
            )

        sentinel_timestamp = datetime(2020, 1, 1, 12, 50, tzinfo=UTC)
        sentinel_location = f"QA-RISK-SENTINEL-{uuid.uuid4().hex[:12]}"
        _publish_temporal_records(
            [
                _publish_weather(
                    sentinel_location,
                    sentinel_timestamp,
                    sentinel_timestamp,
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
            location_id=sentinel_location,
            event_timestamp=_timestamp_text(sentinel_timestamp),
            status="accepted",
            count=1,
        )
        sentinel_ms = _epoch_millis(sentinel_timestamp)
        _wait_for_watermark(job_id, sentinel_ms - _OUT_OF_ORDER_MS - 1)

        logs, metric_records = _wait_for_metric_keys(
            started_at,
            location_ids,
            expected_keys,
            deadline_seconds=30,
        )
        _assert_complete_records(metric_records, expected_metrics, fields=_METRIC_FIELDS)
        deadline = time.monotonic() + 30
        risk_records = _risk_records(logs, location_ids)
        while time.monotonic() < deadline and len(risk_records) < len(expected_assessments):
            time.sleep(0.25)
            logs = _taskmanager_logs_since(started_at)
            risk_records = _risk_records(logs, location_ids)
        _assert_complete_records(
            risk_records,
            expected_assessments,
            fields=_METRIC_FIELDS | _RISK_FIELDS,
        )
        assert _wait_for_assessed_windows(job_id, len(expected_assessments)) >= len(
            expected_assessments
        )

        _trigger_checkpoint(job_id)
        checkpoint_after_restore = _wait_for_completed_checkpoint(
            job_id,
            after_timestamp_ms=restored_started_at_ms,
            deadline_seconds=30,
        )
        assert int(checkpoint_after_restore["trigger_timestamp"]) >= restored_started_at_ms
        latest = _rest_json(f"/jobs/{job_id}/checkpoints").get("latest", {})
        assert int(latest["restored"]["id"]) == checkpoint_id
        assert int(latest["completed"]["id"]) == int(checkpoint_after_restore["id"])
    finally:
        own_job_id = _wait_for_own_job_id(job_name, job_id or submission.get("job_id"))
        if own_job_id:
            current = _rest_json(f"/jobs/{own_job_id}")
            if current.get("name") == job_name:
                _cancel_own_active_job(own_job_id)
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    state = _rest_json(f"/jobs/{own_job_id}").get("state")
                    if state in {"CANCELED", "FAILED", "FINISHED"}:
                        break
                    time.sleep(0.25)
                assert _rest_json(f"/jobs/{own_job_id}").get("state") in {
                    "CANCELED",
                    "FAILED",
                    "FINISHED",
                }
