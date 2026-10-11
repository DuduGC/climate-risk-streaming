from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from business_qa_support import (
    _semantic_equal as _json_values_equal,
)
from business_qa_support import (
    legacy_snapshot_additions as _legacy_snapshot_additions,
)
from business_qa_support import (
    legacy_snapshot_delta as _legacy_snapshot_delta,
)
from test_flink_risk import (
    _expected_profile_records,
    _risk_records,
)
from test_flink_temporal import (
    _assert_checkpoint_readable_on_both_flink_nodes,
    _assert_no_active_jobs,
    _checkpoint_metadata_path,
    _cleanup_unbounded_qa_job,
    _epoch_millis,
    _metric_key,
    _publish_temporal_records,
    _publish_weather,
    _rest_json,
    _source_offsets,
    _submit_unbounded_qa_job,
    _subtask_metric_value,
    _taskmanager_logs_since,
    _timestamp_text,
    _trigger_checkpoint,
    _vertex_id,
    _wait_for_offset_advance,
    _wait_for_qa_audit_count,
    _wait_for_restored_checkpoint,
    _wait_for_source_offsets,
)
from test_kafka_flink import _structured_log_entries

pytestmark = pytest.mark.integration

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_LEGACY_MANIFEST = _REPOSITORY_ROOT / ".venv" / "stage8-legacy-checkpoint.json"
_ACTIVE_JOB_STATES = {"CREATED", "INITIALIZING", "RESTARTING", "RUNNING", "CANCELLING"}
_TERMINAL_JOB_STATES = {"CANCELED", "FAILED", "FINISHED"}
_METRIC_COLUMNS = {
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
    "schema_version",
    "risk_score",
    "risk_level",
    "risk_rules_version",
    "risk_components",
}
_ALERT_COLUMNS = {
    "alert_id",
    "location_id",
    "window_start_ms",
    "window_end_ms",
    "risk_rules_version",
    "risk_score",
    "risk_level",
    "payload",
}
_ALERT_PAYLOAD_FIELDS = {
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
    "risk_score",
    "risk_level",
    "risk_components",
    "risk_rules_version",
    "alert_id",
}
_KNOWN_PROFILES = {
    "LOW": {
        "location_id": "RJ01",
        "precipitation": 0.0,
        "wind_speed": 5.0,
        "wind_gust": 10.0,
        "visibility": 10_000.0,
        "score": 0,
        "level": "LOW",
        "components": {
            "precipitation_rate": 0,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
    },
    "MEDIUM": {
        "location_id": "SP01",
        "precipitation": 0.5,
        "wind_speed": 5.0,
        "wind_gust": 10.0,
        "visibility": 10_000.0,
        "score": 25,
        "level": "MEDIUM",
        "components": {
            "precipitation_rate": 25,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
    },
    "HIGH": {
        "location_id": "CT01",
        "precipitation": 0.0,
        "wind_speed": 40.0,
        "wind_gust": 10.0,
        "visibility": 10_000.0,
        "score": 50,
        "level": "HIGH",
        "components": {
            "precipitation_rate": 0,
            "wind_speed": 50,
            "wind_gust": 0,
            "visibility": 0,
        },
    },
    "CRITICAL": {
        "location_id": "RJ01",
        "precipitation": 0.0,
        "wind_speed": 5.0,
        "wind_gust": 10.0,
        "visibility": 199.0,
        "score": 100,
        "level": "CRITICAL",
        "components": {
            "precipitation_rate": 0,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 100,
        },
    },
}


def _docker() -> str:
    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("docker is required when FLINK_BUSINESS_TEST is enabled", pytrace=False)
    return docker


def _compose(
    *arguments: str,
    input_text: str | None = None,
    timeout: float = 40,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [_docker(), "compose", *arguments],
        cwd=_REPOSITORY_ROOT,
        input=input_text,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        timeout=timeout,
    )
    if check and result.returncode != 0:
        pytest.fail(
            f"Docker Compose QA command failed ({arguments[0]}): {result.stderr[-2000:]}",
            pytrace=False,
        )
    return result


@pytest.fixture
def paused_producer():
    _assert_no_active_jobs()
    _compose("pause", "producer")
    try:
        yield
    finally:
        _compose("unpause", "producer", timeout=30)


def _target_time(suffix: str) -> datetime:
    """Map a per-run hexadecimal suffix to a five-minute slot from 2020–2024."""
    if not suffix or any(character not in "0123456789abcdefABCDEF" for character in suffix):
        raise ValueError("business QA suffix must be hexadecimal")
    start = datetime(2020, 1, 3, 13, 10, tzinfo=UTC)
    stop = datetime(2025, 1, 1, tzinfo=UTC)
    slot_count = int((stop - start).total_seconds()) // 300
    return start + timedelta(minutes=(int(suffix, 16) % slot_count) * 5)


def _profile_expected(
    *,
    location_id: str,
    timestamp: datetime,
    precipitation: float,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
    score: int,
    level: str,
    components: dict[str, int],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    metrics, assessments = _expected_profile_records(
        location_id=location_id,
        timestamp_ms=_epoch_millis(timestamp),
        precipitation=precipitation,
        wind_speed=wind_speed,
        wind_gust=wind_gust,
        visibility=visibility,
        score=score,
        level=level,
        components=dict(components),
    )
    return metrics, assessments


def _fixture_record(
    profile: dict[str, object],
    timestamp: datetime,
    ingestion: datetime,
) -> dict[str, str]:
    return _publish_weather(
        str(profile["location_id"]),
        timestamp,
        ingestion,
        temperature=20.0,
        precipitation=float(profile["precipitation"]),
        interval=900,
        wind_speed=float(profile["wind_speed"]),
        wind_gust=float(profile["wind_gust"]),
        visibility=float(profile["visibility"]),
    )


def _fixture_assessments(
    profile: dict[str, object], timestamp: datetime
) -> list[dict[str, object]]:
    _, assessments = _profile_expected(
        location_id=str(profile["location_id"]),
        timestamp=timestamp,
        precipitation=float(profile["precipitation"]),
        wind_speed=float(profile["wind_speed"]),
        wind_gust=float(profile["wind_gust"]),
        visibility=float(profile["visibility"]),
        score=int(profile["score"]),
        level=str(profile["level"]),
        components=profile["components"],
    )
    return assessments


def _business_fixture(suffix: str) -> dict[str, Any]:
    event_t = _target_time(suffix)
    event_t_plus_60 = event_t + timedelta(minutes=60)
    event_t_plus_100 = event_t + timedelta(minutes=100)
    ingestion = event_t

    low = _KNOWN_PROFILES["LOW"]
    medium = _KNOWN_PROFILES["MEDIUM"]
    high = _KNOWN_PROFILES["HIGH"]
    critical = _KNOWN_PROFILES["CRITICAL"]
    unknown = {
        "location_id": f"QA-UNKNOWN-{suffix}",
        "precipitation": 0.0,
        "wind_speed": 40.0,
        "wind_gust": 10.0,
        "visibility": 10_000.0,
        "score": 50,
        "level": "HIGH",
        "components": {
            "precipitation_rate": 0,
            "wind_speed": 50,
            "wind_gust": 0,
            "visibility": 0,
        },
    }

    low_original = _fixture_record(low, event_t, ingestion)
    low_duplicate = _publish_weather(
        "RJ01",
        event_t,
        ingestion + timedelta(seconds=1),
        temperature=99.0,
        precipitation=999.0,
        interval=1,
        wind_speed=99.0,
        wind_gust=100.0,
        visibility=1.0,
    )
    first_phase = [
        low_original,
        low_duplicate,
        _fixture_record(medium, event_t, ingestion),
        _fixture_record(high, event_t, ingestion),
        _fixture_record(unknown, event_t, ingestion),
    ]
    critical_phase = [_fixture_record(critical, event_t_plus_60, event_t_plus_60)]
    sentinel_phase = [
        _fixture_record(
            low | {"location_id": "CT01"},
            event_t_plus_100,
            event_t_plus_100,
        )
    ]

    target_assessments: list[dict[str, object]] = []
    for profile in (low, medium, high):
        target_assessments.extend(_fixture_assessments(profile, event_t))
    target_assessments.extend(_fixture_assessments(critical, event_t_plus_60))
    unknown_assessments = _fixture_assessments(unknown, event_t)
    sentinel_assessments = _fixture_assessments(
        low | {"location_id": "CT01"}, event_t_plus_100
    )

    return {
        "suffix": suffix,
        "event_t": event_t,
        "event_t_plus_60": event_t_plus_60,
        "event_t_plus_100": event_t_plus_100,
        "first_phase": first_phase,
        "critical_phase": critical_phase,
        "sentinel_phase": sentinel_phase,
        "target_assessments": target_assessments,
        "unknown_assessments": unknown_assessments,
        "sentinel_assessments": sentinel_assessments,
        "cleanup_keys": _assessment_keys(
            target_assessments + unknown_assessments + sentinel_assessments
        ),
    }


def _assessment_key(assessment: dict[str, object]) -> tuple[str, int, int, str]:
    return (
        str(assessment["location_id"]),
        int(assessment["window_start_ms"]),
        int(assessment["window_end_ms"]),
        str(assessment["risk_rules_version"]),
    )


def _assessment_keys(
    assessments: list[dict[str, object]],
) -> list[tuple[str, int, int, str]]:
    return [_assessment_key(assessment) for assessment in assessments]


def _alert_id_oracle(key: tuple[str, int, int, str]) -> str:
    encoded = json.dumps(
        list(key),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _expected_alert(assessment: dict[str, object]) -> dict[str, object]:
    alert = dict(assessment)
    alert["operation"] = "weather_alert"
    alert["alert_id"] = _alert_id_oracle(_assessment_key(assessment))
    return alert


def _eligible_assessments(
    assessments: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [assessment for assessment in assessments if int(assessment["risk_score"]) >= 50]


def _watch_identities(fixture: dict[str, Any]) -> set[tuple[str, int, int, str]]:
    possible = (
        fixture["target_assessments"]
        + fixture["unknown_assessments"]
        + fixture["sentinel_assessments"]
    )
    return {_assessment_key(item) for item in possible}


def _matches_alert_identity(
    payload: dict[str, object],
    identities: set[tuple[str, int, int, str]],
) -> bool:
    start = payload.get("window_start_ms")
    end = payload.get("window_end_ms")
    if type(start) is not int or type(end) is not int:
        return False
    identity = (
        payload.get("location_id"),
        start,
        end,
        payload.get("risk_rules_version"),
    )
    return identity in identities


def _expected_metric_rows(
    assessments: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {key: value for key, value in assessment.items() if key not in {"component", "operation"}}
        for assessment in assessments
    ]


def _expected_alert_rows(
    assessments: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for assessment in _eligible_assessments(assessments):
        alert = _expected_alert(assessment)
        rows.append(
            {
                "alert_id": alert["alert_id"],
                "location_id": alert["location_id"],
                "window_start_ms": alert["window_start_ms"],
                "window_end_ms": alert["window_end_ms"],
                "risk_rules_version": alert["risk_rules_version"],
                "risk_score": alert["risk_score"],
                "risk_level": alert["risk_level"],
                "payload": alert,
            }
        )
    return rows


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _values_table(keys: list[tuple[str, int, int, str]]) -> str:
    if not keys:
        raise ValueError("an exact business identity list is required")
    values = ",\n        ".join(
        f"({_sql_text(location_id)}, {start_ms}, {end_ms}, {_sql_text(version)})"
        for location_id, start_ms, end_ms, version in keys
    )
    return (
        "(VALUES\n        "
        + values
        + "\n    ) AS wanted(location_id, window_start_ms, window_end_ms, risk_rules_version)"
    )


def _psql(script: str, *, application_role: bool) -> subprocess.CompletedProcess[str]:
    if os.environ.get("FLINK_BUSINESS_TEST") != "1":
        pytest.skip("set FLINK_BUSINESS_TEST=1 to access business sink QA")
    if application_role:
        login = (
            'export PGUSER="$APP_DB_USER" PGPASSWORD="$APP_DB_PASSWORD" '
            'PGDATABASE="$POSTGRES_DB" PGHOST=127.0.0.1; '
        )
    else:
        login = (
            'export PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD" '
            'PGDATABASE="$POSTGRES_DB" PGHOST=127.0.0.1; '
        )
    command = (
        login
        + "exec psql --no-psqlrc --quiet --set=ON_ERROR_STOP=1 "
        + "--tuples-only --no-align"
    )
    return _compose(
        "exec",
        "-T",
        "postgres",
        "bash",
        "-c",
        command,
        input_text=script,
        timeout=40,
    )


def _query_rows(
    table: str,
    keys: list[tuple[str, int, int, str]],
    *,
    application_role: bool,
) -> list[dict[str, Any]]:
    if table not in {"weather_metrics", "weather_alerts"}:
        raise ValueError("unsupported business facts table")
    alias = "metric" if table == "weather_metrics" else "alert"
    sql = f"""
SELECT COALESCE(
    jsonb_agg(to_jsonb({alias}) ORDER BY {alias}.location_id, {alias}.window_start_ms,
              {alias}.window_end_ms, {alias}.risk_rules_version),
    '[]'::jsonb
)::text
FROM climate.{table} AS {alias}
JOIN {_values_table(keys)}
  ON {alias}.location_id = wanted.location_id
 AND {alias}.window_start_ms = wanted.window_start_ms
 AND {alias}.window_end_ms = wanted.window_end_ms
 AND {alias}.risk_rules_version = wanted.risk_rules_version;
"""
    result = _psql(sql, application_role=application_role)
    if result.returncode != 0:
        pytest.fail(f"business QA query failed: {result.stderr[-2000:]}", pytrace=False)
    payload = result.stdout.strip() or "[]"
    return json.loads(payload)


def _query_metrics(keys: list[tuple[str, int, int, str]]) -> list[dict[str, Any]]:
    return _query_rows("weather_metrics", keys, application_role=True)


def _query_alerts(keys: list[tuple[str, int, int, str]]) -> list[dict[str, Any]]:
    return _query_rows("weather_alerts", keys, application_role=True)


def _snapshot_all_business_rows() -> dict[str, list[dict[str, Any]]]:
    metrics_sql = """
SELECT COALESCE(
    jsonb_agg(to_jsonb(metric) ORDER BY metric.location_id, metric.window_start_ms,
              metric.window_end_ms, metric.risk_rules_version),
    '[]'::jsonb
)::text
FROM climate.weather_metrics AS metric;
"""
    alerts_sql = """
SELECT COALESCE(
    jsonb_agg(to_jsonb(alert) ORDER BY alert.location_id, alert.window_start_ms,
              alert.window_end_ms, alert.risk_rules_version),
    '[]'::jsonb
)::text
FROM climate.weather_alerts AS alert;
"""
    metric_result = _psql(metrics_sql, application_role=True)
    alert_result = _psql(alerts_sql, application_role=True)
    if metric_result.returncode != 0 or alert_result.returncode != 0:
        pytest.fail("application-role business snapshot failed", pytrace=False)
    return {
        "metrics": json.loads(metric_result.stdout.strip() or "[]"),
        "alerts": json.loads(alert_result.stdout.strip() or "[]"),
    }


def _delete_business_key_sets(
    metric_keys: list[tuple[str, int, int, str]],
    alert_keys: list[tuple[str, int, int, str]],
) -> None:
    statements = ["BEGIN;"]
    if alert_keys:
        statements.append(
            f"""
DELETE FROM climate.weather_alerts AS alert
USING {_values_table(alert_keys)}
WHERE alert.location_id = wanted.location_id
  AND alert.window_start_ms = wanted.window_start_ms
  AND alert.window_end_ms = wanted.window_end_ms
  AND alert.risk_rules_version = wanted.risk_rules_version;
"""
        )
    if metric_keys:
        statements.append(
            f"""
DELETE FROM climate.weather_metrics AS metric
USING {_values_table(metric_keys)}
WHERE metric.location_id = wanted.location_id
  AND metric.window_start_ms = wanted.window_start_ms
  AND metric.window_end_ms = wanted.window_end_ms
  AND metric.risk_rules_version = wanted.risk_rules_version;
"""
        )
    statements.append("COMMIT;")
    result = _psql("\n".join(statements), application_role=False)
    if result.returncode != 0:
        pytest.fail(
            f"fixture-scoped PostgreSQL cleanup failed: {result.stderr[-2000:]}",
            pytrace=False,
        )


def _write_legacy_business_diff(
    *,
    job_id: str,
    checkpoint_id: int,
    before: dict[str, list[dict[str, Any]]],
    after: dict[str, list[dict[str, Any]]],
    metric_keys: set[tuple[str, int, int, str]],
    alert_keys: set[tuple[str, int, int, str]],
) -> Path:
    path = _REPOSITORY_ROOT / ".venv" / "stage8-legacy-business-diff.json"
    proof = {
        "job_id": job_id,
        "restored_checkpoint_id": checkpoint_id,
        "before": before,
        "after": after,
        "new_metric_keys": sorted(metric_keys),
        "new_alert_keys": sorted(alert_keys),
    }
    path.write_text(
        json.dumps(proof, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _delete_fixture_rows(keys: list[tuple[str, int, int, str]]) -> None:
    _delete_business_key_sets(keys, keys)


def _wait_for_exact_rows(
    query,
    expected_keys: set[tuple[str, int, int, str]],
    *,
    deadline_seconds: float,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + deadline_seconds
    latest: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        latest = query()
        observed = {
            (
                str(row["location_id"]),
                int(row["window_start_ms"]),
                int(row["window_end_ms"]),
                str(row["risk_rules_version"]),
            )
            for row in latest
        }
        if observed == expected_keys and len(latest) == len(expected_keys):
            return latest
        time.sleep(0.25)
    raise AssertionError(
        "business rows did not reach the exact fixture identity set; "
        f"expected={sorted(expected_keys)}, observed={latest}"
    )


def _assert_complete_rows(
    actual: list[dict[str, Any]],
    expected: list[dict[str, object]],
    *,
    fields: set[str],
) -> None:
    assert len(actual) == len(expected)
    assert all(set(row) == fields for row in actual)
    assert all(set(row) == fields for row in expected)
    actual_by_key = {_assessment_key(row): row for row in actual}
    expected_by_key = {_assessment_key(row): row for row in expected}
    assert len(actual_by_key) == len(actual), "business sink returned duplicate identities"
    assert actual_by_key.keys() == expected_by_key.keys()
    for key, expected_row in expected_by_key.items():
        assert _json_values_equal(actual_by_key[key], expected_row), (
            f"business row differs for identity {key}: {actual_by_key[key]}"
        )


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _wait_for_unknown_location_count(
    job_id: str,
    expected: int,
    deadline_seconds: float = 120,
) -> int:
    deadline = time.monotonic() + deadline_seconds
    latest: int | None = None
    while time.monotonic() < deadline:
        try:
            vertex = _vertex_id(job_id, "weather-business-records")
            latest = _subtask_metric_value(job_id, vertex, "unknown_locations")
        except AssertionError:
            latest = None
        if latest is not None and latest >= expected:
            return latest
        time.sleep(0.25)
    raise AssertionError(
        f"business_sinks.unknown_locations did not reach {expected}; observed {latest}"
    )


def _publish_in_event_time_order(
    job_id: str,
    fixture: dict[str, Any],
    *,
    started_at: str,
) -> dict[str, int]:
    offsets = _wait_for_source_offsets(job_id)
    _publish_temporal_records(fixture["first_phase"])
    timestamp_t = _timestamp_text(fixture["event_t"])
    for location_id in ("RJ01", "SP01", "CT01", f"QA-UNKNOWN-{fixture['suffix']}"):
        _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=timestamp_t,
            status="accepted",
            count=1,
        )
    _wait_for_qa_audit_count(
        started_at,
        location_id="RJ01",
        event_timestamp=timestamp_t,
        status="duplicate",
        count=1,
    )
    offsets = _wait_for_offset_advance(job_id, offsets)

    _publish_temporal_records(fixture["critical_phase"])
    _wait_for_qa_audit_count(
        started_at,
        location_id="RJ01",
        event_timestamp=_timestamp_text(fixture["event_t_plus_60"]),
        status="accepted",
        count=1,
    )
    offsets = _wait_for_offset_advance(job_id, offsets)

    _publish_temporal_records(fixture["sentinel_phase"])
    _wait_for_qa_audit_count(
        started_at,
        location_id="CT01",
        event_timestamp=_timestamp_text(fixture["event_t_plus_100"]),
        status="accepted",
        count=1,
    )
    return _wait_for_offset_advance(job_id, offsets)


def _business_fixture_replay_records(fixture: dict[str, Any]) -> list[dict[str, str]]:
    return (
        fixture["first_phase"][:1]
        + fixture["first_phase"][2:]
        + fixture["critical_phase"]
        + fixture["sentinel_phase"]
    )


def _wait_for_fixture_replay_duplicates(
    started_at: str,
    fixture: dict[str, Any],
) -> None:
    timestamp_t = _timestamp_text(fixture["event_t"])
    for location_id in ("RJ01", "SP01", "CT01", f"QA-UNKNOWN-{fixture['suffix']}"):
        _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=timestamp_t,
            status="duplicate",
            count=1,
        )
    for timestamp, location_id in (
        (fixture["event_t_plus_60"], "RJ01"),
        (fixture["event_t_plus_100"], "CT01"),
    ):
        _wait_for_qa_audit_count(
            started_at,
            location_id=location_id,
            event_timestamp=_timestamp_text(timestamp),
            status="duplicate",
            count=1,
        )


def _capture_topic_end_offsets(topic: str) -> dict[str, int]:
    config = json.dumps({"topic": topic}, separators=(",", ":"))
    script = """\
import json
import sys
from confluent_kafka import Consumer, TopicPartition

config = json.load(sys.stdin)
topic = config["topic"]
consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "weather-business-observer-offsets",
    "enable.auto.commit": False,
})
metadata = consumer.list_topics(topic, timeout=10)
partitions = metadata.topics[topic].partitions
offsets = {}
for partition in sorted(partitions):
    _, high = consumer.get_watermark_offsets(
        TopicPartition(topic, partition), timeout=10, cached=False
    )
    offsets[str(partition)] = int(high)
consumer.close()
print(json.dumps(offsets, separators=(",", ":")))
"""
    result = _compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "producer",
        "python",
        "-c",
        script,
        input_text=config,
        timeout=30,
    )
    return {
        str(partition): int(offset)
        for partition, offset in json.loads(result.stdout.strip()).items()
    }


def _capture_alert_end_offsets() -> dict[str, int]:
    return _capture_topic_end_offsets("weather.alerts")


def _capture_group_committed_offsets(
    group_id: str,
    *,
    topic: str,
    expected_offsets: dict[str, int | None] | None = None,
    deadline_seconds: float = 10,
) -> dict[str, int | None]:
    config = json.dumps(
        {
            "group_id": group_id,
            "topic": topic,
            "partitions": [0, 1, 2],
            "expected_offsets": expected_offsets,
            "deadline_seconds": deadline_seconds,
        },
        separators=(",", ":"),
    )
    script = """\
import json
import sys
import time
from confluent_kafka import Consumer, TopicPartition

config = json.load(sys.stdin)
consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": config["group_id"],
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
})
topic_partitions = [
    TopicPartition(config["topic"], int(partition))
    for partition in config["partitions"]
]
expected = config["expected_offsets"]
deadline = time.monotonic() + float(config["deadline_seconds"])
offsets = {}
while time.monotonic() < deadline:
    remaining = deadline - time.monotonic()
    committed = consumer.committed(topic_partitions, timeout=min(5, remaining))
    offsets = {
        str(item.partition): (int(item.offset) if int(item.offset) >= 0 else None)
        for item in committed
    }
    if expected is None or offsets == {
        str(key): (None if value is None else int(value))
        for key, value in expected.items()
    }:
        break
    time.sleep(0.25)
consumer.close()
if expected is not None and offsets != {
    str(key): (None if value is None else int(value))
    for key, value in expected.items()
}:
    raise SystemExit("Kafka group offsets did not reach the expected checkpoint positions")
print(json.dumps(offsets, separators=(",", ":")))
"""
    result = _compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "producer",
        "python",
        "-c",
        script,
        input_text=config,
        timeout=deadline_seconds + 30,
    )
    raw = json.loads(result.stdout.strip())
    return {
        str(partition): None if offset is None else int(offset)
        for partition, offset in raw.items()
    }


def _wait_for_alert_identities(
    start_offsets: dict[str, int],
    expected_assessments: list[dict[str, object]],
    *,
    deadline_seconds: float = 180,
) -> list[dict[str, Any]]:
    expected_identities = sorted(
        [_assessment_key(assessment) for assessment in _eligible_assessments(expected_assessments)]
    )
    config = json.dumps(
        {
            "start_offsets": start_offsets,
            "expected_identities": expected_identities,
            "deadline_seconds": deadline_seconds,
        },
        separators=(",", ":"),
    )
    script = """\
import json
import sys
import time
from confluent_kafka import Consumer, KafkaError, TopicPartition

config = json.load(sys.stdin)
expected = {tuple(identity) for identity in config["expected_identities"]}
consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "weather-business-observer-" + str(time.time_ns()),
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
consumer.assign([
    TopicPartition("weather.alerts", int(partition), int(offset))
    for partition, offset in config["start_offsets"].items()
])
found = []
seen = set()
deadline = time.monotonic() + float(config["deadline_seconds"])
while time.monotonic() < deadline and not expected.issubset(seen):
    message = consumer.poll(0.25)
    if message is None:
        continue
    if message.error() is not None:
        if message.error().code() == KafkaError._PARTITION_EOF:
            continue
        raise RuntimeError(str(message.error()))
    try:
        payload = json.loads(message.value())
    except (TypeError, ValueError):
        continue
    identity = (
        payload.get("location_id"),
        payload.get("window_start_ms"),
        payload.get("window_end_ms"),
        payload.get("risk_rules_version"),
    )
    if identity in expected:
        seen.add(identity)
        found.append({
            "partition": message.partition(),
            "offset": message.offset(),
            "key": message.key().decode("utf-8") if message.key() is not None else None,
            "payload": payload,
        })
consumer.close()
if not expected.issubset(seen):
    raise SystemExit("expected alert identities were not acknowledged by Kafka")
print(json.dumps(found, ensure_ascii=False, separators=(",", ":")))
"""
    result = _compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "producer",
        "python",
        "-c",
        script,
        input_text=config,
        timeout=deadline_seconds + 30,
    )
    return json.loads(result.stdout.strip())


def _consume_alert_range(
    start_offsets: dict[str, int],
    end_offsets: dict[str, int],
    watch_identities: set[tuple[str, int, int, str]],
    *,
    deadline_seconds: float = 60,
) -> list[dict[str, Any]]:
    config = json.dumps(
        {
            "start_offsets": start_offsets,
            "end_offsets": end_offsets,
            "watch_identities": sorted(watch_identities),
            "deadline_seconds": deadline_seconds,
        },
        separators=(",", ":"),
    )
    script = """\
import json
import sys
import time
from confluent_kafka import Consumer, KafkaError, TopicPartition

config = json.load(sys.stdin)
starts = {int(key): int(value) for key, value in config["start_offsets"].items()}
ends = {int(key): int(value) for key, value in config["end_offsets"].items()}
watch = {tuple(identity) for identity in config["watch_identities"]}
consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "weather-business-range-" + str(time.time_ns()),
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
consumer.assign([
    TopicPartition("weather.alerts", partition, starts[partition])
    for partition in sorted(starts)
])
finished = {partition for partition in starts if starts[partition] >= ends[partition]}
records = []
deadline = time.monotonic() + float(config["deadline_seconds"])
while len(finished) < len(starts) and time.monotonic() < deadline:
    message = consumer.poll(0.25)
    if message is None:
        continue
    partition = message.partition()
    if message.error() is not None:
        if message.error().code() == KafkaError._PARTITION_EOF:
            if message.offset() >= ends[partition]:
                finished.add(partition)
            continue
        raise RuntimeError(str(message.error()))
    if message.offset() >= ends[partition]:
        finished.add(partition)
        continue
    try:
        payload = json.loads(message.value())
    except (TypeError, ValueError):
        continue
    identity = (
        payload.get("location_id"),
        payload.get("window_start_ms"),
        payload.get("window_end_ms"),
        payload.get("risk_rules_version"),
    )
    if identity in watch:
        records.append({
            "partition": partition,
            "offset": message.offset(),
            "key": message.key().decode("utf-8") if message.key() is not None else None,
            "payload": payload,
        })
    if message.offset() + 1 >= ends[partition]:
        finished.add(partition)
consumer.close()
if len(finished) != len(starts):
    raise SystemExit("Kafka observer did not reach the captured end offsets")
print(json.dumps(records, ensure_ascii=False, separators=(",", ":")))
"""
    result = _compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "producer",
        "python",
        "-c",
        script,
        input_text=config,
        timeout=deadline_seconds + 30,
    )
    return json.loads(result.stdout.strip())


def _assert_alert_messages(
    records: list[dict[str, Any]],
    expected_assessments: list[dict[str, object]],
    *,
    minimum_per_id: int = 1,
    require_all_ids: bool = True,
) -> None:
    expected_by_id = {
        str(alert["alert_id"]): alert
        for alert in (_expected_alert(item) for item in _eligible_assessments(expected_assessments))
    }
    counts: Counter[str] = Counter()
    for record in records:
        payload = record["payload"]
        alert_id = str(payload.get("alert_id"))
        assert alert_id in expected_by_id
        expected = expected_by_id[alert_id]
        assert record["key"] == expected["location_id"]
        assert set(payload) == _ALERT_PAYLOAD_FIELDS
        assert _json_values_equal(payload, expected)
        counts[alert_id] += 1
    if require_all_ids:
        assert set(counts) == set(expected_by_id)
    else:
        assert set(counts).issubset(expected_by_id)
    assert all(count >= minimum_per_id for count in counts.values())


def _wait_for_terminal_job(
    job_id: str,
    job_name: str,
    deadline_seconds: float,
    *,
    observed_states: list[str] | None = None,
) -> str:
    deadline = time.monotonic() + deadline_seconds
    last_state = "unknown"
    while time.monotonic() < deadline:
        job = _rest_json(f"/jobs/{job_id}")
        if job.get("name") != job_name:
            raise AssertionError(f"unexpected Flink job identity for {job_id}")
        last_state = str(job.get("state"))
        if observed_states is not None and (
            not observed_states or observed_states[-1] != last_state
        ):
            observed_states.append(last_state)
        if last_state in _TERMINAL_JOB_STATES:
            return last_state
        time.sleep(0.25)
    raise AssertionError(f"job {job_name} did not finish before its deadline: {last_state}")


def _trigger_and_wait_checkpoint(job_id: str, *, deadline_seconds: float = 30) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    request = _trigger_checkpoint(job_id)
    trigger_id = request.get("request-id") or request.get("requestId")
    if not trigger_id:
        raise AssertionError("manual checkpoint request returned no request id")

    while time.monotonic() < deadline:
        operation = _rest_json(f"/jobs/{job_id}/checkpoints/{trigger_id}")
        operation_status = operation.get("status", {}).get("id")
        operation_details = operation.get("operation") or {}
        if (
            operation_status in {"FAILED", "DECLINED", "CANCELED"}
            or operation_details.get("failureCause") is not None
        ):
            raise AssertionError("manual checkpoint request did not complete")
        checkpoint_id = operation_details.get("checkpointId")
        if operation_status == "COMPLETED" and checkpoint_id is not None:
            details = _rest_json(
                f"/jobs/{job_id}/checkpoints/details/{checkpoint_id}"
            )
            details_status = details.get("status")
            if (
                details_status == "COMPLETED"
                and str(details.get("id")) == str(checkpoint_id)
            ):
                return details
        time.sleep(0.2)
    raise AssertionError("manual checkpoint request was not completed before its deadline")


def _assert_restored_and_completed(
    job_id: str,
    *,
    restored_id: int,
    completed_id: int,
) -> None:
    latest = _rest_json(f"/jobs/{job_id}/checkpoints").get("latest", {})
    restored = latest.get("restored")
    completed = latest.get("completed")
    assert restored is not None and int(restored["id"]) == restored_id
    assert completed is not None and int(completed["id"]) == completed_id
    assert int(completed["id"]) != restored_id


def _assert_no_sensitive_failure_text(text: str) -> None:
    lowered = text.casefold()
    for marker in ("app_db_password", "postgres_password", "password="):
        if marker in lowered:
            raise AssertionError("sensitive failure text detected in QA output")


def _assert_diagnostics_for_unknown(
    logs: str,
    *,
    location_id: str,
    expected_count: int,
) -> None:
    risk = [
        record
        for record in _risk_records(logs, {location_id})
        if record.get("operation") == "risk_assessment"
    ]
    assert len(risk) == expected_count
    business_rejections = [
        record
        for _, record in _structured_log_entries(logs)
        if record.get("component") == "flink"
        and record.get("operation") == "business_sinks"
        and record.get("location_id") == location_id
        and record.get("reason") == "unknown_location"
        and record.get("status") == "rejected"
    ]
    assert len(business_rejections) == expected_count


def _start_time_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _wait_for_business_outputs(
    job_id: str,
    fixture: dict[str, Any],
    *,
    deadline_seconds: float = 180,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    deadline = time.monotonic() + deadline_seconds
    target = fixture["target_assessments"]
    keys = _assessment_keys(target)
    key_set = set(keys)
    metrics = _wait_for_exact_rows(
        lambda: _query_metrics(keys),
        key_set,
        deadline_seconds=_remaining_business_output_seconds(deadline),
    )
    alerts = _wait_for_exact_rows(
        lambda: _query_alerts(keys),
        {
            _assessment_key(item)
            for item in _eligible_assessments(target)
        },
        deadline_seconds=_remaining_business_output_seconds(deadline),
    )
    if fixture["unknown_assessments"]:
        _wait_for_unknown_location_count(
            job_id,
            expected=len(fixture["unknown_assessments"]),
            deadline_seconds=_remaining_business_output_seconds(deadline),
        )
    return metrics, alerts


def _remaining_business_output_seconds(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AssertionError("shared business output deadline expired")
    return remaining


def _assert_fixture_absent(fixture: dict[str, Any]) -> None:
    assert _query_metrics(fixture["cleanup_keys"]) == []
    assert _query_alerts(fixture["cleanup_keys"]) == []


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AssertionError("fixture timestamps must include UTC timezone")
    return parsed.astimezone(UTC)


def _load_legacy_checkpoint_manifest() -> dict[str, Any]:
    if not _LEGACY_MANIFEST.is_file():
        pytest.fail("the preserved Stage 7 checkpoint manifest is required", pytrace=False)
    manifest = json.loads(_LEGACY_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["origin_stage"] == 7
    assert manifest["has_business_sinks"] is False
    assert manifest["checkpoint"]["status"] == "COMPLETED"
    assert manifest["metadata_path"] == (
        "/opt/flink/checkpoints/e0fc5a701089bc01b09f8de036b5f6b5/"
        "chk-2/_metadata"
    )
    assert manifest["job_id"] == "e0fc5a701089bc01b09f8de036b5f6b5"
    return manifest


def _legacy_fixture(manifest: dict[str, Any]) -> dict[str, Any]:
    profiles = {str(profile["level"]): profile for profile in manifest["profiles"]}
    assert set(profiles) == {"HIGH", "LOW"}
    high_event = json.loads(profiles["HIGH"]["record"]["value"])
    low_event = json.loads(profiles["LOW"]["record"]["value"])
    high_time = _parse_timestamp(high_event["event_timestamp"])
    low_time = _parse_timestamp(low_event["event_timestamp"])
    assert high_time == low_time
    assert high_time == _parse_timestamp(high_event["ingestion_timestamp"])
    assert low_time == _parse_timestamp(low_event["ingestion_timestamp"])
    assert high_event["location_id"] == "RJ01"
    assert low_event["location_id"] == "SP01"

    expected_profiles = {
        "HIGH": {
            "score": 50,
            "level": "HIGH",
            "components": {
                "precipitation_rate": 0,
                "wind_speed": 50,
                "wind_gust": 0,
                "visibility": 0,
            },
        },
        "LOW": {
            "score": 0,
            "level": "LOW",
            "components": {
                "precipitation_rate": 0,
                "wind_speed": 0,
                "wind_gust": 0,
                "visibility": 0,
            },
        },
    }
    assessments: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    records: list[dict[str, str]] = []
    for level, event, manifest_profile in (
        ("HIGH", high_event, profiles["HIGH"]),
        ("LOW", low_event, profiles["LOW"]),
    ):
        oracle = expected_profiles[level]
        profile_metrics, profile_assessments = _profile_expected(
            location_id=str(event["location_id"]),
            timestamp=high_time,
            precipitation=float(event["precipitation"]),
            wind_speed=float(event["wind_speed"]),
            wind_gust=float(event["wind_gust"]),
            visibility=float(event["visibility"]),
            score=int(oracle["score"]),
            level=str(oracle["level"]),
            components=oracle["components"],
        )
        metrics.extend(profile_metrics)
        assessments.extend(profile_assessments)
        revised = dict(event)
        revised.update(
            {
                "temperature": 99.0,
                "precipitation": 999.0,
                "measurement_interval_seconds": 1,
                "wind_speed": 99.0,
                "wind_gust": 100.0,
                "visibility": 1.0,
            }
        )
        records.append(
            {
                "key": str(manifest_profile["record"]["key"]),
                "value": json.dumps(revised, ensure_ascii=False, separators=(",", ":")),
            }
        )

    sentinel_time = high_time + timedelta(minutes=100)
    sentinel_profile = _KNOWN_PROFILES["LOW"] | {"location_id": "CT01"}
    sentinel = _publish_weather(
        "CT01",
        sentinel_time,
        sentinel_time,
        temperature=20.0,
        precipitation=0.0,
        interval=900,
        wind_speed=5.0,
        wind_gust=10.0,
        visibility=10_000.0,
    )
    return {
        "profiles": profiles,
        "event_t": high_time,
        "revisions": records,
        "sentinel": sentinel,
        "metrics": metrics,
        "assessments": assessments,
        "sentinel_assessments": _fixture_assessments(sentinel_profile, sentinel_time),
        "cleanup_keys": _assessment_keys(
            assessments + _fixture_assessments(sentinel_profile, sentinel_time)
        ),
    }


def _start_business_job(
    job_name: str,
    group_id: str,
    *,
    submission: dict[str, str | None],
    checkpoint_interval_seconds: int,
    restore_metadata: str | None = None,
    python_entrypoint: str | None = None,
) -> str:
    return _submit_unbounded_qa_job(
        job_name,
        group_id,
        submission=submission,
        checkpoint_interval_seconds=checkpoint_interval_seconds,
        restore_metadata=restore_metadata,
        business_sinks=True,
        python_entrypoint=python_entrypoint,
    )


@pytest.mark.skipif(
    os.environ.get("FLINK_BUSINESS_TEST") != "1",
    reason="set FLINK_BUSINESS_TEST=1 to exercise Flink business sinks",
)
def test_real_flink_business_delivery_and_checkpoint_replay_are_idempotent(paused_producer):
    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-business{suffix}"
    group_id = f"{job_name}-group"
    fixture = _business_fixture(suffix)
    target = fixture["target_assessments"]
    target_keys = _assessment_keys(target)
    expected_metrics = _expected_metric_rows(target)
    expected_alerts = _expected_alert_rows(target)
    expected_alert_ids = {
        _alert_id_oracle(_assessment_key(item))
        for item in _eligible_assessments(target)
    }
    watch_identities = _watch_identities(fixture)
    submission: dict[str, str | None] = {"job_id": None}
    active_job_id: str | None = None
    initial_job_id: str | None = None
    restored_job_id: str | None = None
    restored_submission: dict[str, str | None] = {"job_id": None}
    checkpoint: dict[str, Any] | None = None

    _assert_fixture_absent(fixture)
    try:
        started_at = _start_time_text()
        active_job_id = _start_business_job(
            job_name,
            group_id,
            submission=submission,
            checkpoint_interval_seconds=15,
        )
        source_before = _wait_for_source_offsets(active_job_id)
        alert_offsets_before = _capture_alert_end_offsets()
        final_offsets = _publish_in_event_time_order(
            active_job_id,
            fixture,
            started_at=started_at,
        )
        assert any(
            final_offsets.get(metric, -1) > source_before.get(metric, -1)
            for metric in final_offsets
        )

        metric_rows, alert_rows = _wait_for_business_outputs(active_job_id, fixture)
        _assert_complete_rows(metric_rows, expected_metrics, fields=_METRIC_COLUMNS)
        _assert_complete_rows(alert_rows, expected_alerts, fields=_ALERT_COLUMNS)
        assert len(metric_rows) == 24
        assert len(alert_rows) == 12

        unknown_keys = _assessment_keys(fixture["unknown_assessments"])
        sentinel_keys = _assessment_keys(fixture["sentinel_assessments"])
        assert _query_metrics(unknown_keys) == []
        assert _query_alerts(unknown_keys) == []
        assert _query_metrics(sentinel_keys) == []
        logs = _taskmanager_logs_since(started_at)
        _assert_diagnostics_for_unknown(
            logs,
            location_id=f"QA-UNKNOWN-{suffix}",
            expected_count=6,
        )

        _wait_for_alert_identities(alert_offsets_before, target)
        first_end_offsets = _capture_alert_end_offsets()
        original_alert_messages = _consume_alert_range(
            alert_offsets_before,
            first_end_offsets,
            watch_identities,
        )
        _assert_alert_messages(original_alert_messages, target)
        assert Counter(item["payload"]["alert_id"] for item in original_alert_messages) == Counter(
            {alert_id: 1 for alert_id in expected_alert_ids}
        )

        raw_end_at_checkpoint = _capture_topic_end_offsets("weather.raw")
        checkpoint = _trigger_and_wait_checkpoint(active_job_id)
        checkpoint_id = int(checkpoint["id"])
        checkpoint_metadata = _checkpoint_metadata_path(checkpoint)
        _assert_checkpoint_readable_on_both_flink_nodes(checkpoint_metadata)
        offsets_at_checkpoint = _source_offsets(active_job_id)
        assert len(offsets_at_checkpoint) == 3
        committed_at_checkpoint = _capture_group_committed_offsets(
            group_id,
            topic="weather.raw",
            expected_offsets=raw_end_at_checkpoint,
            deadline_seconds=15,
        )

        initial_job_id = active_job_id
        _cleanup_unbounded_qa_job(job_name, initial_job_id, submission)
        active_job_id = None
        submission["job_id"] = None
        restored_job_id = _start_business_job(
            job_name,
            group_id,
            submission=restored_submission,
            checkpoint_interval_seconds=15,
            restore_metadata=checkpoint_metadata,
        )
        assert restored_job_id != initial_job_id
        restored_checkpoint = _wait_for_restored_checkpoint(restored_job_id, checkpoint_id)
        assert int(restored_checkpoint["id"]) == checkpoint_id
        _assert_checkpoint_readable_on_both_flink_nodes(checkpoint_metadata)
        assert (
            _capture_group_committed_offsets(
                group_id,
                topic="weather.raw",
                expected_offsets=committed_at_checkpoint,
                deadline_seconds=10,
            )
            == committed_at_checkpoint
        )

        replay_started_at = _start_time_text()
        assert _capture_topic_end_offsets("weather.raw") == raw_end_at_checkpoint
        _publish_temporal_records(_business_fixture_replay_records(fixture))
        _wait_for_fixture_replay_duplicates(replay_started_at, fixture)
        raw_end_after_replay = _capture_topic_end_offsets("weather.raw")
        assert any(
            raw_end_after_replay[partition] > raw_end_at_checkpoint[partition]
            for partition in raw_end_at_checkpoint
        )

        replay_metric_rows = _query_metrics(target_keys)
        replay_alert_rows = _query_alerts(target_keys)
        _assert_complete_rows(replay_metric_rows, expected_metrics, fields=_METRIC_COLUMNS)
        _assert_complete_rows(replay_alert_rows, expected_alerts, fields=_ALERT_COLUMNS)
        assert len(replay_metric_rows) == 24
        assert len(replay_alert_rows) == 12
        checkpoint_after_replay = _trigger_and_wait_checkpoint(restored_job_id)
        checkpoint_after_replay_id = int(checkpoint_after_replay["id"])
        assert checkpoint_after_replay_id != checkpoint_id
        _assert_restored_and_completed(
            restored_job_id,
            restored_id=checkpoint_id,
            completed_id=checkpoint_after_replay_id,
        )
        committed_after_replay = _capture_group_committed_offsets(
            group_id,
            topic="weather.raw",
            expected_offsets=raw_end_after_replay,
            deadline_seconds=15,
        )
        assert committed_after_replay == raw_end_after_replay
        replay_end_offsets = _capture_alert_end_offsets()
        replay_messages = _consume_alert_range(
            first_end_offsets,
            replay_end_offsets,
            watch_identities,
        )
        _assert_alert_messages(replay_messages, target, require_all_ids=False)
        assert all(
            item["payload"]["alert_id"] in expected_alert_ids
            for item in replay_messages
        )
    finally:
        if restored_job_id is not None or restored_submission.get("job_id") is not None:
            _cleanup_unbounded_qa_job(
                job_name,
                restored_job_id,
                restored_submission,
            )
        elif active_job_id is not None or submission.get("job_id") is not None:
            _cleanup_unbounded_qa_job(job_name, active_job_id, submission)
        _delete_fixture_rows(fixture["cleanup_keys"])
        _assert_fixture_absent(fixture)


@pytest.mark.skipif(
    os.environ.get("FLINK_BUSINESS_TEST") != "1",
    reason="set FLINK_BUSINESS_TEST=1 to restore the preserved Stage 7 checkpoint",
)
def test_real_flink_business_restores_stage7_checkpoint_and_writes_open_windows(
    paused_producer,
):
    manifest = _load_legacy_checkpoint_manifest()
    fixture = _legacy_fixture(manifest)
    expected = fixture["assessments"]
    keys = _assessment_keys(expected)
    expected_metrics = _expected_metric_rows(expected)
    expected_alerts = _expected_alert_rows(expected)
    expected_alert_ids = {
        _alert_id_oracle(_assessment_key(item))
        for item in _eligible_assessments(expected)
    }
    watch_identities = {
        _assessment_key(item)
        for item in expected + fixture["sentinel_assessments"]
    }
    submission: dict[str, str | None] = {"job_id": None}
    job_id: str | None = None
    business_before = _snapshot_all_business_rows()
    expected_rows_observed = False

    _assert_checkpoint_readable_on_both_flink_nodes(manifest["metadata_path"])
    _assert_fixture_absent(fixture)
    try:
        alert_offsets_before = _capture_alert_end_offsets()
        raw_offsets_before_restore = _capture_topic_end_offsets("weather.raw")
        started_at = _start_time_text()
        job_id = _start_business_job(
            str(manifest["job_name"]),
            str(manifest["group_id"]),
            submission=submission,
            checkpoint_interval_seconds=int(manifest["checkpoint_interval_seconds"]),
            restore_metadata=str(manifest["metadata_path"]),
        )
        assert job_id != manifest["job_id"]
        legacy_checkpoint_id = int(manifest["checkpoint"]["id"])
        restored = _wait_for_restored_checkpoint(job_id, legacy_checkpoint_id)
        assert int(restored["id"]) == legacy_checkpoint_id
        source_before = _wait_for_source_offsets(job_id)

        _publish_temporal_records(fixture["revisions"])
        timestamp = _timestamp_text(fixture["event_t"])
        for location_id in ("RJ01", "SP01"):
            _wait_for_qa_audit_count(
                started_at,
                location_id=location_id,
                event_timestamp=timestamp,
                status="duplicate",
                count=1,
            )
        offsets_after_revisions = _wait_for_offset_advance(job_id, source_before)

        _publish_temporal_records([fixture["sentinel"]])
        _wait_for_qa_audit_count(
            started_at,
            location_id="CT01",
            event_timestamp=_timestamp_text(fixture["event_t"] + timedelta(minutes=100)),
            status="accepted",
            count=1,
        )
        offsets_after_sentinel = _wait_for_offset_advance(job_id, offsets_after_revisions)
        assert any(
            offsets_after_sentinel.get(metric, -1) > source_before.get(metric, -1)
            for metric in source_before
        )

        metric_rows, alert_rows = _wait_for_business_outputs(
            job_id,
            {"target_assessments": expected, "unknown_assessments": []},
            deadline_seconds=180,
        )
        _assert_complete_rows(metric_rows, expected_metrics, fields=_METRIC_COLUMNS)
        _assert_complete_rows(alert_rows, expected_alerts, fields=_ALERT_COLUMNS)
        assert len(metric_rows) == 12
        assert len(alert_rows) == 6
        expected_rows_observed = True

        logs = _taskmanager_logs_since(started_at)
        _wait_for_qa_audit_count(
            started_at,
            location_id="RJ01",
            event_timestamp=timestamp,
            status="duplicate",
            count=1,
        )
        duplicate_audits = [
            record
            for _, record in _structured_log_entries(logs)
            if record.get("operation") == "data_quality"
            and record.get("status") == "duplicate"
            and record.get("event_timestamp") == timestamp
            and record.get("location_id") in {"RJ01", "SP01"}
        ]
        assert Counter(record["location_id"] for record in duplicate_audits) == Counter(
            {"RJ01": 1, "SP01": 1}
        )
        expected_keys = set(_assessment_keys(expected))
        expected_metric_keys = {
            (location_id, start_ms, end_ms)
            for location_id, start_ms, end_ms, _ in expected_keys
        }
        metric_records = [
            record
            for _, record in _structured_log_entries(logs)
            if record.get("component") == "flink"
            and record.get("operation") == "window_aggregate"
            and _metric_key(record) in expected_metric_keys
        ]
        assert len(metric_records) == 12
        assert Counter(_canonical(record) for record in metric_records) == Counter(
            _canonical(record) for record in fixture["metrics"]
        )
        risk_records = [
            record
            for record in _risk_records(logs, {"RJ01", "SP01"})
            if _assessment_key(record) in expected_keys
        ]
        assert len(risk_records) == 12
        assert Counter(_canonical(record) for record in risk_records) == Counter(
            _canonical(record) for record in expected
        )

        _wait_for_alert_identities(alert_offsets_before, expected)
        raw_offsets_after_inputs = _capture_topic_end_offsets("weather.raw")
        assert any(
            raw_offsets_after_inputs[partition] > raw_offsets_before_restore[partition]
            for partition in raw_offsets_before_restore
        )
        _trigger_and_wait_checkpoint(job_id)
        committed_offsets_after_restore = _capture_group_committed_offsets(
            str(manifest["group_id"]),
            topic="weather.raw",
            expected_offsets=raw_offsets_after_inputs,
            deadline_seconds=30,
        )
        assert committed_offsets_after_restore == raw_offsets_after_inputs
        latest_checkpoints = _rest_json(f"/jobs/{job_id}/checkpoints").get("latest", {})
        completed_checkpoint = latest_checkpoints.get("completed")
        restored_checkpoint = latest_checkpoints.get("restored")
        assert restored_checkpoint is not None
        assert int(restored_checkpoint["id"]) == legacy_checkpoint_id
        assert completed_checkpoint is not None
        checkpoint8_id = int(completed_checkpoint["id"])
        assert checkpoint8_id != legacy_checkpoint_id
        checkpoint8_metadata = _checkpoint_metadata_path(completed_checkpoint)
        _assert_checkpoint_readable_on_both_flink_nodes(checkpoint8_metadata)

        end_offsets = _capture_alert_end_offsets()
        messages = _consume_alert_range(
            alert_offsets_before,
            end_offsets,
            watch_identities,
        )
        _assert_alert_messages(messages, expected)
        assert Counter(item["payload"]["alert_id"] for item in messages) == Counter(
            {alert_id: 1 for alert_id in expected_alert_ids}
        )
    finally:
        if job_id is not None or submission.get("job_id") is not None:
            _cleanup_unbounded_qa_job(str(manifest["job_name"]), job_id, submission)

        business_after = _snapshot_all_business_rows()
        new_metric_keys, new_alert_keys = _legacy_snapshot_additions(
            business_before,
            business_after,
        )
        if expected_rows_observed:
            expected_alert_keys = {
                _assessment_key(item)
                for item in _eligible_assessments(expected)
            }
            assert set(keys).issubset(new_metric_keys)
            assert expected_alert_keys.issubset(new_alert_keys)
        _write_legacy_business_diff(
            job_id=str(job_id or submission.get("job_id") or "unsubmitted"),
            checkpoint_id=int(manifest["checkpoint"]["id"]),
            before=business_before,
            after=business_after,
            metric_keys=new_metric_keys,
            alert_keys=new_alert_keys,
        )
        _delete_business_key_sets(
            sorted(new_metric_keys),
            sorted(new_alert_keys),
        )
        after_cleanup = _snapshot_all_business_rows()
        remaining_metric_keys, remaining_alert_keys = _legacy_snapshot_delta(
            business_before,
            after_cleanup,
        )
        assert remaining_metric_keys == set()
        assert remaining_alert_keys == set()
        _assert_fixture_absent(fixture)
