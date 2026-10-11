from __future__ import annotations

import importlib
import json
import subprocess
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def business_helpers(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    integration_directory = Path(__file__).resolve().parents[1] / "integration"
    monkeypatch.syspath_prepend(str(integration_directory))
    return importlib.import_module("test_flink_business")


@pytest.mark.unit
def test_business_fixture_uses_unique_historical_event_times_and_original_ingestion(
    business_helpers: ModuleType,
) -> None:
    first = business_helpers._business_fixture("000000000001")
    second = business_helpers._business_fixture("000000000002")

    first_t = first["event_t"]
    second_t = second["event_t"]
    assert first_t.year in {2020, 2021, 2022, 2023, 2024}
    assert second_t.year in {2020, 2021, 2022, 2023, 2024}
    assert first_t != second_t

    records = (
        first["first_phase"][:1]
        + first["first_phase"][2:]
        + first["critical_phase"]
        + first["sentinel_phase"]
    )
    for record in records:
        payload = json.loads(record["value"])
        assert payload["event_timestamp"] == payload["ingestion_timestamp"]
        assert payload["event_timestamp"].endswith("Z")
        assert payload["location_id"] in {"RJ01", "SP01", "CT01", "QA-UNKNOWN-000000000001"}

    first_original = json.loads(first["first_phase"][0]["value"])
    duplicate = json.loads(first["first_phase"][1]["value"])
    assert duplicate["event_timestamp"] == first_original["event_timestamp"]
    duplicate_ingestion = datetime.fromisoformat(duplicate["ingestion_timestamp"])
    original_ingestion = datetime.fromisoformat(first_original["ingestion_timestamp"])
    assert duplicate_ingestion - original_ingestion == timedelta(seconds=1)


@pytest.mark.unit
def test_alert_observer_selects_fixture_identity_even_when_alert_hash_is_wrong(
    business_helpers: ModuleType,
) -> None:
    identity = ("RJ01", 1_700_000_000_000, 1_700_001_800_000, "mvp-1")
    wrong_hash_payload = {
        "location_id": "RJ01",
        "window_start_ms": 1_700_000_000_000,
        "window_end_ms": 1_700_001_800_000,
        "risk_rules_version": "mvp-1",
        "alert_id": "0" * 64,
    }
    wrong_identity_payload = {
        **wrong_hash_payload,
        "location_id": "SP01",
    }

    matches = business_helpers._matches_alert_identity
    assert matches(wrong_hash_payload, {identity}) is True
    assert matches(wrong_identity_payload, {identity}) is False


@pytest.mark.unit
def test_database_row_comparison_accepts_integral_float_and_rejects_boolean(
    business_helpers: ModuleType,
) -> None:
    actual = {
        "location_id": "RJ01",
        "window_start_ms": 1,
        "window_end_ms": 1_800_001,
        "event_count": 1,
        "temperature_avg": 20,
        "precipitation_max": 0.0,
        "precipitation_rate_max_mm_per_hour": 0.0,
        "wind_speed_max": 5.0,
        "wind_gust_max": 10.0,
        "visibility_min": 10_000.0,
        "measurement_interval_min_seconds": 900,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1,
        "last_event_timestamp_ms": 1,
        "schema_version": 1,
        "risk_score": 0,
        "risk_level": "LOW",
        "risk_rules_version": "mvp-1",
        "risk_components": {
            "precipitation_rate": 0,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
    }
    expected = deepcopy(actual)
    expected["temperature_avg"] = 20.0

    business_helpers._assert_complete_rows(
        [actual],
        [expected],
        fields=business_helpers._METRIC_COLUMNS,
    )

    boolean_score = deepcopy(actual)
    boolean_score["risk_components"]["wind_speed"] = False
    with pytest.raises(AssertionError):
        business_helpers._assert_complete_rows(
            [boolean_score],
            [expected],
            fields=business_helpers._METRIC_COLUMNS,
        )


@pytest.mark.unit
def test_committed_offset_probe_uses_the_qa_group_and_expected_partition_offsets(
    business_helpers: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"0": 17, "1": 23, "2": 9}
    observed: dict[str, object] = {}

    def compose(*arguments: str, input_text: str | None = None, **kwargs):
        observed["arguments"] = arguments
        observed["input"] = input_text
        return subprocess.CompletedProcess(
            arguments,
            0,
            stdout=json.dumps(expected),
            stderr="",
        )

    monkeypatch.setattr(business_helpers, "_compose", compose)

    result = business_helpers._capture_group_committed_offsets(
        "weather-flink-qa-offsetprobe-group",
        topic="weather.raw",
        expected_offsets=expected,
        deadline_seconds=7,
    )

    assert result == expected
    assert json.loads(str(observed["input"])) == {
        "group_id": "weather-flink-qa-offsetprobe-group",
        "topic": "weather.raw",
        "partitions": [0, 1, 2],
        "expected_offsets": expected,
        "deadline_seconds": 7,
    }


@pytest.mark.unit
def test_legacy_snapshot_delta_keeps_preexisting_rows_immutable_and_isolates_new_keys(
    business_helpers: ModuleType,
) -> None:
    prior_metric = {
        "location_id": "RJ01",
        "window_start_ms": 1_700_000_000_000,
        "window_end_ms": 1_700_001_800_000,
        "risk_rules_version": "mvp-1",
        "risk_score": 0,
        "temperature_avg": 20.0,
    }
    new_metric = {
        "location_id": "SP01",
        "window_start_ms": 1_700_000_000_000,
        "window_end_ms": 1_700_001_800_000,
        "risk_rules_version": "mvp-1",
        "risk_score": 50,
        "temperature_avg": 21.0,
    }
    new_alert = {
        "location_id": "SP01",
        "window_start_ms": 1_700_000_000_000,
        "window_end_ms": 1_700_001_800_000,
        "risk_rules_version": "mvp-1",
        "alert_id": "a" * 64,
    }
    before = {"metrics": [prior_metric], "alerts": []}
    after = {"metrics": [prior_metric, new_metric], "alerts": [new_alert]}

    new_metric_keys, new_alert_keys = business_helpers._legacy_snapshot_delta(before, after)

    assert new_metric_keys == {
        ("SP01", 1_700_000_000_000, 1_700_001_800_000, "mvp-1")
    }
    assert new_alert_keys == {
        ("SP01", 1_700_000_000_000, 1_700_001_800_000, "mvp-1")
    }

    changed_prior = {**prior_metric, "temperature_avg": 99.0}
    with pytest.raises(AssertionError, match="pre-existing business row changed"):
        business_helpers._legacy_snapshot_delta(
            before,
            {"metrics": [changed_prior, new_metric], "alerts": [new_alert]},
        )


@pytest.mark.unit
def test_fault_output_guard_detects_a_secret_without_rendering_its_value(
    business_helpers: ModuleType,
) -> None:
    fault_helpers = importlib.import_module("test_flink_business_faults")
    dummy_password = "stage8-test-secret-value"

    with pytest.raises(AssertionError, match="sensitive credential") as error:
        fault_helpers._assert_no_password_values_in_text(
            f"JDBC failure password={dummy_password}",
            [dummy_password],
        )

    assert dummy_password not in str(error.value)
    fault_helpers._assert_no_password_values_in_text(
        "JDBC connection refused",
        [dummy_password],
    )


@pytest.mark.unit
def test_fault_fixture_seeds_low_state_before_c_and_reuses_one_real_kafka_key(
    business_helpers: ModuleType,
) -> None:
    fault_helpers = importlib.import_module("test_flink_business_faults")
    fixture = fault_helpers._fault_fixture("000000000011")

    assert fixture["event_t"] - fixture["seed_t"] == timedelta(minutes=60)
    assert fixture["sentinel_t"] - fixture["event_t"] == timedelta(minutes=100)
    assert len(fixture["target_assessments"]) == 12
    assert len(fault_helpers.business._eligible_assessments(fixture["target_assessments"])) == 6
    assert all(record["key"] == "RJ01" for record in fixture["records_after_checkpoint"])

    seed = json.loads(fixture["seed_record"]["value"])
    after_checkpoint = [
        json.loads(record["value"])
        for record in fixture["records_after_checkpoint"]
    ]
    assert seed["location_id"] == "RJ01"
    assert seed["event_timestamp"] == seed["ingestion_timestamp"]
    assert after_checkpoint[0]["event_timestamp"] == seed["event_timestamp"]
    assert after_checkpoint[1]["event_timestamp"] == business_helpers._timestamp_text(
        fixture["event_t"]
    )
    assert after_checkpoint[2]["event_timestamp"] == business_helpers._timestamp_text(
        fixture["sentinel_t"]
    )


@pytest.mark.unit
def test_fault_cause_guard_requires_the_selected_sink_and_rejects_taskmanager_oom(
    business_helpers: ModuleType,
) -> None:
    fault_helpers = importlib.import_module("test_flink_business_faults")
    out_of_memory = (
        "java.lang.OutOfMemoryError: Metaspace; "
        "at org.apache.flink.connector.kafka.source.reader.KafkaSourceReader.pollNext"
    )

    assert not fault_helpers._matches_expected_sink_failure("jdbc", out_of_memory)
    assert not fault_helpers._matches_expected_sink_failure("kafka", out_of_memory)
    assert not fault_helpers._matches_expected_sink_failure(
        "kafka",
        "org.apache.kafka.common.errors.TimeoutException: topic weather.alerts; "
        "bootstrap kafka:9092 unavailable",
    )
    assert fault_helpers._matches_expected_sink_failure(
        "jdbc",
        "unable to open JDBC writer: org.postgresql.util.PSQLException: "
        "Connection to postgres:1 refused; java.net.ConnectException: Connection refused",
    )
    assert fault_helpers._matches_expected_sink_failure(
        "kafka",
        "org.apache.kafka.common.errors.TimeoutException: topic weather.alerts; broker kafka:1",
    )


@pytest.mark.unit
def test_business_output_wait_skips_unknown_counter_when_fixture_has_no_unknowns(
    business_helpers: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = [
        {
            "location_id": "RJ01",
            "window_start_ms": index * 1_800_000,
            "window_end_ms": (index + 1) * 1_800_000,
            "risk_rules_version": "mvp-1",
            "risk_score": 70 if index < 6 else 20,
        }
        for index in range(12)
    ]
    metric_rows = [{"row": index} for index in range(12)]
    alert_rows = [{"row": index} for index in range(6)]
    results = iter((metric_rows, alert_rows))

    monkeypatch.setattr(
        business_helpers,
        "_wait_for_exact_rows",
        lambda *_args, **_kwargs: next(results),
    )
    monkeypatch.setattr(
        business_helpers,
        "_wait_for_unknown_location_count",
        lambda *_args, **_kwargs: pytest.fail("empty fixture must not poll an unexported counter"),
    )

    actual_metrics, actual_alerts = business_helpers._wait_for_business_outputs(
        "qa-job",
        {"target_assessments": target, "unknown_assessments": []},
        deadline_seconds=1,
    )

    assert actual_metrics == metric_rows
    assert actual_alerts == alert_rows


@pytest.mark.unit
def test_business_output_wait_shares_one_deadline_across_all_sink_observers(
    business_helpers: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = [
        {
            "location_id": "RJ01",
            "window_start_ms": index * 1_800_000,
            "window_end_ms": (index + 1) * 1_800_000,
            "risk_rules_version": "mvp-1",
            "risk_score": 70 if index < 6 else 20,
        }
        for index in range(12)
    ]
    clock = [0.0]
    deadlines: list[float] = []
    unknown_calls: list[bool] = []
    monkeypatch.setattr(business_helpers.time, "monotonic", lambda: clock[0])

    def wait_for_rows(_query, _keys, *, deadline_seconds: float):
        deadlines.append(deadline_seconds)
        clock[0] += 0.6
        return []

    def wait_for_unknowns(*_args, **_kwargs):
        unknown_calls.append(True)
        return 1

    monkeypatch.setattr(business_helpers, "_wait_for_exact_rows", wait_for_rows)
    monkeypatch.setattr(
        business_helpers,
        "_wait_for_unknown_location_count",
        wait_for_unknowns,
    )

    with pytest.raises(AssertionError, match="shared business output deadline expired"):
        business_helpers._wait_for_business_outputs(
            "qa-job",
            {"target_assessments": target, "unknown_assessments": [{}]},
            deadline_seconds=1,
        )

    assert deadlines == pytest.approx([1.0, 0.4])
    assert unknown_calls == []


@pytest.mark.unit
def test_sensitive_failure_guard_raises_generic_error_without_echoing_text(
    business_helpers: ModuleType,
) -> None:
    sensitive_text = "password=qa-unit-dummy-secret"

    with pytest.raises(AssertionError) as error:
        business_helpers._assert_no_sensitive_failure_text(sensitive_text)

    assert str(error.value) == "sensitive failure text detected in QA output"
    assert sensitive_text not in str(error.value)
    assert "qa-unit-dummy-secret" not in str(error.value)


@pytest.mark.unit
def test_manual_checkpoint_wait_correlates_request_id_not_latest_global_checkpoint(
    business_helpers: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    periodic_checkpoint = {
        "id": 1,
        "status": {"id": "COMPLETED"},
        "trigger_timestamp": 1_000,
    }
    manual_checkpoint = {
        "id": 2,
        "status": "COMPLETED",
        "trigger_timestamp": 2_000,
    }
    operation_responses = iter(
        (
            {"status": {"id": "IN_PROGRESS"}, "operation": None},
            {
                "status": {"id": "COMPLETED"},
                "operation": {"checkpointId": 2, "failureCause": None},
            },
        )
    )
    paths: list[str] = []

    def rest_json(path: str):
        paths.append(path)
        if path == "/jobs/qa-job/checkpoints":
            return {"latest": {"completed": periodic_checkpoint}}
        if path == "/jobs/qa-job/checkpoints/manual-request":
            return next(operation_responses)
        if path == "/jobs/qa-job/checkpoints/details/2":
            return manual_checkpoint
        raise AssertionError("unexpected checkpoint REST path")

    monkeypatch.setattr(
        business_helpers,
        "_trigger_checkpoint",
        lambda _job_id: {"request-id": "manual-request"},
    )
    monkeypatch.setattr(business_helpers, "_rest_json", rest_json)
    monkeypatch.setattr(business_helpers.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        business_helpers,
        "_wait_for_completed_checkpoint",
        lambda *_args, **_kwargs: periodic_checkpoint,
        raising=False,
    )

    completed = business_helpers._trigger_and_wait_checkpoint(
        "qa-job",
        deadline_seconds=1,
    )

    assert completed["id"] == 2
    assert "/jobs/qa-job/checkpoints/manual-request" in paths
    assert "/jobs/qa-job/checkpoints/details/2" in paths

    monkeypatch.setattr(
        business_helpers,
        "_trigger_checkpoint",
        lambda _job_id: {"request-id": "failed-request"},
    )
    monkeypatch.setattr(
        business_helpers,
        "_rest_json",
        lambda _path: {
            "status": {"id": "FAILED"},
            "operation": {"checkpointId": None, "failureCause": "sensitive detail"},
        },
    )
    with pytest.raises(AssertionError) as error:
        business_helpers._trigger_and_wait_checkpoint("qa-job", deadline_seconds=1)
    assert str(error.value) == "manual checkpoint request did not complete"
    assert "sensitive detail" not in str(error.value)


@pytest.mark.unit
def test_expired_fault_recovery_budget_raises_before_starting_wait(
    business_helpers: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fault_helpers = importlib.import_module("test_flink_business_faults")
    monkeypatch.setattr(fault_helpers.time, "monotonic", lambda: 10.0)
    wait_calls: list[float] = []

    def wait_for_job(*, deadline_seconds: float) -> None:
        wait_calls.append(deadline_seconds)

    with pytest.raises(AssertionError) as error:
        fault_helpers._wait_with_fault_budget(wait_for_job, 9.0)

    assert str(error.value) == "shared sink fault and recovery budget expired"
    assert wait_calls == []
