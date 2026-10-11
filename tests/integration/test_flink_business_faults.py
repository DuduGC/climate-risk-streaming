from __future__ import annotations

import os
import re
import subprocess
import time
import uuid
from collections import Counter
from datetime import timedelta
from typing import Any

import pytest
import test_flink_business as business

pytestmark = pytest.mark.integration


@pytest.fixture
def paused_producer():
    business._assert_no_active_jobs()
    business._compose("pause", "producer")
    try:
        yield
    finally:
        business._compose("unpause", "producer", timeout=30)


def _fault_entrypoint(path: str, sink_kind: str) -> None:
    if sink_kind not in {"jdbc", "kafka"}:
        raise ValueError("unsupported QA sink fault")
    if re.fullmatch(r"/tmp/weather-flink-qa-[A-Za-z0-9][A-Za-z0-9_.-]*\.py", path) is None:
        raise ValueError("fault entrypoint must stay in the QA temporary namespace")
    source = f'''from dataclasses import replace

from climate_logistics.business_config import BusinessSinkSettings
from climate_logistics.flink_job import (
    JobSettings,
    _arguments,
    _validate_execution_identity,
    build_pipeline,
)

arguments = _arguments()
if not arguments.qa_audit or not arguments.qa_business_sinks:
    raise SystemExit("fault entrypoint requires audited business QA")
settings = JobSettings(
    bootstrap_servers=arguments.bootstrap_servers,
    group_id=arguments.group_id,
    qa_audit=arguments.qa_audit,
    checkpoint_interval_seconds=arguments.qa_checkpoint_interval_seconds or 60,
)
_validate_execution_identity(
    arguments.job_name,
    settings,
    checkpoint_interval_override=arguments.qa_checkpoint_interval_seconds is not None,
)
business = BusinessSinkSettings.from_environment(
    bootstrap_servers=settings.bootstrap_servers
)
if {sink_kind!r} == "jdbc":
    business = replace(business, port=1)
else:
    business = replace(business, kafka_bootstrap_servers="kafka:1")
build_pipeline(settings, business_sinks=business).execute(arguments.job_name)
'''
    business._compose(
        "exec",
        "-T",
        "flink-jobmanager",
        "bash",
        "-c",
        f"umask 022; cat > {path}; chmod 0644 {path}",
        input_text=source,
    )


def _remove_fault_entrypoint(path: str) -> None:
    business._compose("exec", "-T", "flink-jobmanager", "rm", "-f", path)


def _fault_fixture(suffix: str) -> dict[str, Any]:
    event_t = business._target_time(suffix)
    seed_t = event_t - timedelta(minutes=60)
    sentinel_t = event_t + timedelta(minutes=100)
    high = business._KNOWN_PROFILES["HIGH"] | {"location_id": "RJ01"}
    low = business._KNOWN_PROFILES["LOW"] | {"location_id": "RJ01"}
    _, seed_assessments = business._profile_expected(
        location_id="RJ01",
        timestamp=seed_t,
        precipitation=float(low["precipitation"]),
        wind_speed=float(low["wind_speed"]),
        wind_gust=float(low["wind_gust"]),
        visibility=float(low["visibility"]),
        score=int(low["score"]),
        level=str(low["level"]),
        components=low["components"],
    )
    _, high_assessments = business._profile_expected(
        location_id="RJ01",
        timestamp=event_t,
        precipitation=float(high["precipitation"]),
        wind_speed=float(high["wind_speed"]),
        wind_gust=float(high["wind_gust"]),
        visibility=float(high["visibility"]),
        score=int(high["score"]),
        level=str(high["level"]),
        components=high["components"],
    )
    target_assessments = seed_assessments + high_assessments
    sentinel_assessments = business._fixture_assessments(low, sentinel_t)
    seed = business._publish_weather(
        "RJ01",
        seed_t,
        seed_t,
        temperature=20.0,
        precipitation=0.0,
        interval=900,
        wind_speed=5.0,
        wind_gust=10.0,
        visibility=10_000.0,
    )
    high_record = business._publish_weather(
        "RJ01",
        event_t,
        event_t,
        temperature=20.0,
        precipitation=0.0,
        interval=900,
        wind_speed=40.0,
        wind_gust=10.0,
        visibility=10_000.0,
    )
    sentinel_record = business._publish_weather(
        "RJ01",
        sentinel_t,
        sentinel_t,
        temperature=20.0,
        precipitation=0.0,
        interval=900,
        wind_speed=5.0,
        wind_gust=10.0,
        visibility=10_000.0,
    )
    records_after_checkpoint = [seed, high_record, sentinel_record]
    return {
        "suffix": suffix,
        "seed_t": seed_t,
        "event_t": event_t,
        "sentinel_t": sentinel_t,
        "seed_record": seed,
        "records_after_checkpoint": records_after_checkpoint,
        "target_assessments": target_assessments,
        "unknown_assessments": [],
        "sentinel_assessments": sentinel_assessments,
        "cleanup_keys": business._assessment_keys(target_assessments + sentinel_assessments),
    }


def _publish_fault_fixture(
    job_id: str,
    fixture: dict[str, Any],
    *,
    started_at: str,
) -> dict[str, int]:
    offsets_before = business._wait_for_source_offsets(job_id)
    business._publish_temporal_records(fixture["records_after_checkpoint"])
    statuses = (
        (fixture["seed_t"], "duplicate"),
        (fixture["event_t"], "accepted"),
        (fixture["sentinel_t"], "accepted"),
    )
    for timestamp, status in statuses:
        business._wait_for_qa_audit_count(
            started_at,
            location_id="RJ01",
            event_timestamp=business._timestamp_text(timestamp),
            status=status,
            count=1,
        )
    return business._wait_for_offset_advance(job_id, offsets_before)


def _publish_seed_before_checkpoint(
    job_id: str,
    fixture: dict[str, Any],
    *,
    started_at: str,
) -> dict[str, int]:
    offsets_before = business._wait_for_source_offsets(job_id)
    business._publish_temporal_records([fixture["seed_record"]])
    business._wait_for_qa_audit_count(
        started_at,
        location_id="RJ01",
        event_timestamp=business._timestamp_text(fixture["seed_t"]),
        status="accepted",
        count=1,
    )
    return business._wait_for_offset_advance(job_id, offsets_before)


def _start_expected_fault_job(
    job_name: str,
    group_id: str,
    *,
    submission: dict[str, str | None],
    restore_metadata: str,
    python_entrypoint: str,
) -> str:
    try:
        return business._start_business_job(
            job_name,
            group_id,
            submission=submission,
            checkpoint_interval_seconds=60,
            restore_metadata=restore_metadata,
            python_entrypoint=python_entrypoint,
        )
    except (AssertionError, subprocess.CalledProcessError):
        captured_job_id = submission.get("job_id")
        if captured_job_id is None:
            raise
        return captured_job_id


def _assert_restart_was_observed(
    job_id: str,
    observed_states: list[str],
    logs: str,
) -> None:
    if "RESTARTING" in observed_states or "restarting" in logs.casefold():
        return
    timestamps = business._rest_json(f"/jobs/{job_id}").get("timestamps", {})
    assert int(timestamps.get("RESTARTING", 0)) > 0


def _assert_no_password_values_in_text(text: str, password_values: list[str]) -> None:
    if any(value and value in text for value in password_values):
        raise AssertionError("sensitive credential found in QA failure output")


def _remaining_fault_budget_seconds(
    deadline: float,
    *,
    maximum_seconds: float | None = None,
) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AssertionError("shared sink fault and recovery budget expired")
    return remaining if maximum_seconds is None else min(maximum_seconds, remaining)


def _wait_with_fault_budget(
    waiter,
    deadline: float,
    *args,
    maximum_seconds: float | None = None,
    **kwargs,
):
    kwargs["deadline_seconds"] = _remaining_fault_budget_seconds(
        deadline,
        maximum_seconds=maximum_seconds,
    )
    return waiter(*args, **kwargs)


def _matches_expected_sink_failure(sink_kind: str, failure_text: str) -> bool:
    lowered = failure_text.casefold()
    if any(
        marker in lowered
        for marker in (
            "outofmemoryerror",
            "java heap space",
            "unable to create new native thread",
        )
    ):
        return False
    if sink_kind == "jdbc":
        return (
            "unable to open jdbc writer" in lowered
            and "postgres:1" in lowered
            and any(
                marker in lowered
                for marker in (
                    "psqlexception",
                    "sqlexception",
                    "connectexception",
                    "connection refused",
                    "could not connect",
                )
            )
        )
    if sink_kind == "kafka":
        return (
            "weather.alerts" in lowered
            and "kafka:1" in lowered
            and any(
                marker in lowered
                for marker in (
                    "timeoutexception",
                    "networkexception",
                    "connectexception",
                    "failed to update metadata",
                )
            )
        )
    return False


def _assert_no_runtime_passwords_in_text(text: str) -> None:
    script = r"""while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ -n "${APP_DB_PASSWORD:-}" && "$line" == *"$APP_DB_PASSWORD"* ]]; then
    printf '%s' 'exposed'
    exit 0
  fi
  if [[ -n "${POSTGRES_PASSWORD:-}" && "$line" == *"$POSTGRES_PASSWORD"* ]]; then
    printf '%s' 'exposed'
    exit 0
  fi
done
printf '%s' 'clean'
"""
    result = business._compose(
        "exec",
        "-T",
        "postgres",
        "bash",
        "-c",
        script,
        input_text=text,
        timeout=15,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != "clean":
        raise AssertionError("sensitive credential found or QA failure text could not be checked")


def _assert_no_completed_checkpoint_after(
    job_id: str,
    *,
    checkpoint_timestamp_ms: int,
) -> None:
    latest = business._rest_json(f"/jobs/{job_id}/checkpoints").get("latest", {})
    completed = latest.get("completed")
    if completed is not None:
        assert int(completed["trigger_timestamp"]) <= checkpoint_timestamp_ms


def _wait_for_replayed_inputs(
    started_at: str,
    fixture: dict[str, Any],
    *,
    deadline: float,
) -> None:
    statuses = (
        (fixture["seed_t"], "duplicate"),
        (fixture["event_t"], "accepted"),
        (fixture["sentinel_t"], "accepted"),
    )
    for timestamp, status in statuses:
        _wait_with_fault_budget(
            business._wait_for_qa_audit_count,
            deadline,
            started_at,
            location_id="RJ01",
            event_timestamp=business._timestamp_text(timestamp),
            status=status,
            count=1,
            maximum_seconds=30,
        )


@pytest.mark.parametrize("sink_kind", ["jdbc", "kafka"])
@pytest.mark.skipif(
    os.environ.get("FLINK_BUSINESS_TEST") != "1"
    or os.environ.get("FLINK_BUSINESS_FAULT_TEST") != "1",
    reason=(
        "set FLINK_BUSINESS_TEST=1 and FLINK_BUSINESS_FAULT_TEST=1 "
        "to run real sink fault recovery"
    ),
)
def test_real_flink_business_sink_failure_replays_checkpoint_and_converges(
    sink_kind: str,
    paused_producer,
):
    suffix = uuid.uuid4().hex[:12]
    job_name = f"weather-flink-qa-businessfault{sink_kind}{suffix}"
    group_id = f"{job_name}-group"
    fixture = _fault_fixture(suffix)
    target = fixture["target_assessments"]
    target_keys = business._assessment_keys(target)
    cleanup_keys = fixture["cleanup_keys"]
    expected_metrics = business._expected_metric_rows(target)
    expected_alerts = business._expected_alert_rows(target)
    expected_alert_ids = {
        business._alert_id_oracle(business._assessment_key(item))
        for item in business._eligible_assessments(target)
    }
    watch_identities = business._watch_identities(fixture)
    fault_path = f"/tmp/weather-flink-qa-fault-{sink_kind}-{suffix}.py"

    initial_submission: dict[str, str | None] = {"job_id": None}
    initial_job_id: str | None = None
    fault_submission: dict[str, str | None] = {"job_id": None}
    fault_job_id: str | None = None
    recovery_submission: dict[str, str | None] = {"job_id": None}
    recovery_job_id: str | None = None
    initial_job_id_for_comparison: str | None = None

    business._assert_fixture_absent(fixture)
    try:
        initial_started_at = business._start_time_text()
        initial_job_id = business._start_business_job(
            job_name,
            group_id,
            submission=initial_submission,
            checkpoint_interval_seconds=60,
        )
        source_before = business._wait_for_source_offsets(initial_job_id)
        assert len(source_before) == 3
        seed_offsets = _publish_seed_before_checkpoint(
            initial_job_id,
            fixture,
            started_at=initial_started_at,
        )
        raw_end_at_checkpoint = business._capture_topic_end_offsets("weather.raw")
        checkpoint_c = business._trigger_and_wait_checkpoint(initial_job_id)
        checkpoint_c_id = int(checkpoint_c["id"])
        checkpoint_c_timestamp = int(checkpoint_c["trigger_timestamp"])
        source_offsets_at_c = business._source_offsets(initial_job_id)
        assert source_offsets_at_c == seed_offsets
        committed_at_checkpoint = business._capture_group_committed_offsets(
            group_id,
            topic="weather.raw",
            expected_offsets=raw_end_at_checkpoint,
            deadline_seconds=15,
        )
        checkpoint_c_metadata = business._checkpoint_metadata_path(checkpoint_c)
        business._assert_checkpoint_readable_on_both_flink_nodes(checkpoint_c_metadata)
        alert_offsets_before = business._capture_alert_end_offsets()

        final_input_offsets = _publish_fault_fixture(
            initial_job_id,
            fixture,
            started_at=initial_started_at,
        )
        raw_end_after_input = business._capture_topic_end_offsets("weather.raw")
        assert any(
            raw_end_after_input[partition] > raw_end_at_checkpoint[partition]
            for partition in raw_end_at_checkpoint
        )
        assert any(
            final_input_offsets.get(metric, -1) > source_offsets_at_c.get(metric, -1)
            for metric in source_offsets_at_c
        )
        metric_rows, alert_rows = business._wait_for_business_outputs(
            initial_job_id,
            fixture,
            deadline_seconds=180,
        )
        business._assert_complete_rows(
            metric_rows,
            expected_metrics,
            fields=business._METRIC_COLUMNS,
        )
        business._assert_complete_rows(
            alert_rows,
            expected_alerts,
            fields=business._ALERT_COLUMNS,
        )
        assert len(metric_rows) == 12
        assert len(alert_rows) == 6
        business._wait_for_alert_identities(alert_offsets_before, target)
        latest_completed = business._rest_json(
            f"/jobs/{initial_job_id}/checkpoints"
        ).get("latest", {}).get("completed")
        assert latest_completed is not None
        assert int(latest_completed["id"]) == checkpoint_c_id
        assert time.time_ns() // 1_000_000 - checkpoint_c_timestamp < 60_000

        initial_job_id_for_comparison = initial_job_id
        business._cleanup_unbounded_qa_job(job_name, initial_job_id, initial_submission)
        initial_job_id = None
        initial_submission["job_id"] = None
        first_end_offsets = business._capture_alert_end_offsets()
        first_messages = business._consume_alert_range(
            alert_offsets_before,
            first_end_offsets,
            watch_identities,
        )
        business._assert_alert_messages(first_messages, target)
        assert Counter(item["payload"]["alert_id"] for item in first_messages) == Counter(
            {alert_id: 1 for alert_id in expected_alert_ids}
        )
        _fault_entrypoint(fault_path, sink_kind)

        fault_deadline = time.monotonic() + 240
        fault_started_at = business._start_time_text()
        fault_job_id = _start_expected_fault_job(
            job_name,
            group_id,
            submission=fault_submission,
            restore_metadata=checkpoint_c_metadata,
            python_entrypoint=fault_path,
        )
        assert fault_job_id != initial_job_id_for_comparison
        restored_fault = _wait_with_fault_budget(
            business._wait_for_restored_checkpoint,
            fault_deadline,
            fault_job_id,
            checkpoint_c_id,
            maximum_seconds=60,
        )
        assert int(restored_fault["id"]) == checkpoint_c_id
        observed_states: list[str] = []
        terminal_state = _wait_with_fault_budget(
            business._wait_for_terminal_job,
            fault_deadline,
            fault_job_id,
            job_name,
            observed_states=observed_states,
        )
        assert terminal_state == "FAILED"

        exception_response = business._rest_json(f"/jobs/{fault_job_id}/exceptions")
        exception_text = business._canonical(exception_response)
        fault_logs = business._taskmanager_logs_since(fault_started_at)
        _assert_no_runtime_passwords_in_text(exception_text + fault_logs)
        business._assert_no_sensitive_failure_text(exception_text + fault_logs)
        _assert_restart_was_observed(fault_job_id, observed_states, fault_logs)
        assert _matches_expected_sink_failure(
            sink_kind,
            f"{exception_text}\n{fault_logs}",
        ), f"fault did not originate from the configured {sink_kind} sink destination"
        _assert_no_completed_checkpoint_after(
            fault_job_id,
            checkpoint_timestamp_ms=checkpoint_c_timestamp,
        )
        fault_restored = business._rest_json(
            f"/jobs/{fault_job_id}/checkpoints"
        ).get("latest", {}).get("restored")
        assert fault_restored is not None
        assert int(fault_restored["id"]) == checkpoint_c_id
        assert (
            _wait_with_fault_budget(
                business._capture_group_committed_offsets,
                fault_deadline,
                group_id,
                topic="weather.raw",
                expected_offsets=committed_at_checkpoint,
                maximum_seconds=10,
            )
            == committed_at_checkpoint
        )

        fault_metric_rows = business._query_metrics(target_keys)
        fault_alert_rows = business._query_alerts(target_keys)
        business._assert_complete_rows(
            fault_metric_rows,
            expected_metrics,
            fields=business._METRIC_COLUMNS,
        )
        business._assert_complete_rows(
            fault_alert_rows,
            expected_alerts,
            fields=business._ALERT_COLUMNS,
        )
        assert len(fault_metric_rows) == 12
        assert len(fault_alert_rows) == 6

        recovery_offsets_before = business._capture_alert_end_offsets()
        recovery_started_at = business._start_time_text()
        recovery_job_id = business._start_business_job(
            job_name,
            group_id,
            submission=recovery_submission,
            checkpoint_interval_seconds=60,
            restore_metadata=checkpoint_c_metadata,
        )
        assert recovery_job_id != fault_job_id
        restored_recovery = _wait_with_fault_budget(
            business._wait_for_restored_checkpoint,
            fault_deadline,
            recovery_job_id,
            checkpoint_c_id,
            maximum_seconds=60,
        )
        assert int(restored_recovery["id"]) == checkpoint_c_id
        _wait_for_replayed_inputs(
            recovery_started_at,
            fixture,
            deadline=fault_deadline,
        )
        final_metric_rows, final_alert_rows = _wait_with_fault_budget(
            business._wait_for_business_outputs,
            fault_deadline,
            recovery_job_id,
            fixture,
            maximum_seconds=180,
        )
        business._assert_complete_rows(
            final_metric_rows,
            expected_metrics,
            fields=business._METRIC_COLUMNS,
        )
        business._assert_complete_rows(
            final_alert_rows,
            expected_alerts,
            fields=business._ALERT_COLUMNS,
        )
        assert len(final_metric_rows) == 12
        assert len(final_alert_rows) == 6
        _wait_with_fault_budget(
            business._wait_for_alert_identities,
            fault_deadline,
            recovery_offsets_before,
            target,
            maximum_seconds=180,
        )

        checkpoint_after_recovery = _wait_with_fault_budget(
            business._trigger_and_wait_checkpoint,
            fault_deadline,
            recovery_job_id,
            maximum_seconds=30,
        )
        checkpoint_after_recovery_id = int(checkpoint_after_recovery["id"])
        assert checkpoint_after_recovery_id != checkpoint_c_id
        recovery_metadata = business._checkpoint_metadata_path(checkpoint_after_recovery)
        business._assert_checkpoint_readable_on_both_flink_nodes(recovery_metadata)
        business._assert_restored_and_completed(
            recovery_job_id,
            restored_id=checkpoint_c_id,
            completed_id=checkpoint_after_recovery_id,
        )
        committed_after_recovery = _wait_with_fault_budget(
            business._capture_group_committed_offsets,
            fault_deadline,
            group_id,
            topic="weather.raw",
            expected_offsets=raw_end_after_input,
            maximum_seconds=15,
        )
        assert committed_after_recovery == raw_end_after_input

        end_offsets = business._capture_alert_end_offsets()
        final_messages = _wait_with_fault_budget(
            business._consume_alert_range,
            fault_deadline,
            alert_offsets_before,
            end_offsets,
            watch_identities,
            maximum_seconds=60,
        )
        business._assert_alert_messages(final_messages, target, minimum_per_id=2)
        counts = Counter(item["payload"]["alert_id"] for item in final_messages)
        assert all(counts[alert_id] >= 2 for alert_id in expected_alert_ids)
        _remaining_fault_budget_seconds(fault_deadline)
    finally:
        if initial_job_id is not None or initial_submission.get("job_id") is not None:
            business._cleanup_unbounded_qa_job(
                job_name,
                initial_job_id,
                initial_submission,
            )
        if fault_job_id is not None or fault_submission.get("job_id") is not None:
            business._cleanup_unbounded_qa_job(
                job_name,
                fault_job_id,
                fault_submission,
            )
        if recovery_job_id is not None or recovery_submission.get("job_id") is not None:
            business._cleanup_unbounded_qa_job(
                job_name,
                recovery_job_id,
                recovery_submission,
            )
        _remove_fault_entrypoint(fault_path)
        business._delete_fixture_rows(cleanup_keys)
        business._assert_fixture_absent(fixture)
