from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest


class _CounterFixture:
    def __init__(self) -> None:
        self.value = 0

    def inc(self) -> None:
        self.value += 1


class _MetricGroupFixture:
    def __init__(self) -> None:
        self.groups: dict[str, _MetricGroupFixture] = {}
        self.counters: dict[str, _CounterFixture] = {}

    def add_group(self, name: str) -> _MetricGroupFixture:
        group = self.groups.setdefault(name, _MetricGroupFixture())
        return group

    def counter(self, name: str) -> _CounterFixture:
        return self.counters.setdefault(name, _CounterFixture())


class _StreamingRuntimeContextFixture:
    def __init__(self) -> None:
        self.metrics = _MetricGroupFixture()

    def get_metrics_group(self) -> _MetricGroupFixture:
        return self.metrics


@pytest.mark.unit
def test_open_registers_both_counters_on_streaming_context_metrics():
    pytest.importorskip(
        "pyflink",
        reason="PyFlink is installed only in the Flink runtime image",
    )
    from climate_logistics.flink_job import _ParseWeatherEvent

    context = _StreamingRuntimeContextFixture()
    function = _ParseWeatherEvent()

    function.open(context)

    weather_metrics = context.metrics.groups["weather_ingestion"]
    assert set(weather_metrics.counters) == {"parsed_records", "invalid_records"}
    data_quality_metrics = context.metrics.groups["data_quality"]
    assert set(data_quality_metrics.counters) == {"temporal_rejected_records"}


def _wire_event(
    *, location_id: str = "QA-RJ", event_timestamp: datetime, ingestion_timestamp: datetime
) -> str:
    payload = {
        "location_id": location_id,
        "city": "QA Rio",
        "latitude": -22.9,
        "longitude": -43.17,
        "temperature": 27.4,
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
    }
    return json.dumps(payload, separators=(",", ":"))


@pytest.mark.unit
def test_parser_rejects_temporal_record_without_marker_then_accepts_valid_copy():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from climate_logistics.flink_job import _ParseWeatherEvent

    observed_at = datetime(2026, 10, 9, 12, tzinfo=UTC)
    event_timestamp = observed_at - timedelta(seconds=5)
    invalid = _wire_event(
        event_timestamp=event_timestamp,
        ingestion_timestamp=observed_at + timedelta(minutes=6),
    )
    corrected = _wire_event(
        event_timestamp=event_timestamp,
        ingestion_timestamp=event_timestamp + timedelta(seconds=1),
    )
    context = _StreamingRuntimeContextFixture()
    function = _ParseWeatherEvent(observed_at=observed_at)
    function.open(context)

    assert list(function.flat_map(invalid)) == []
    accepted_copy = list(function.flat_map(corrected))

    assert len(accepted_copy) == 1
    assert context.metrics.groups["weather_ingestion"].counters["parsed_records"].value == 2
    assert context.metrics.groups["weather_ingestion"].counters["invalid_records"].value == 1
    assert context.metrics.groups["data_quality"].counters["temporal_rejected_records"].value == 1


class _MapStateFixture:
    def __init__(self) -> None:
        self.values: dict[str, bool] = {}
        self.reads: list[str] = []
        self.writes: list[tuple[str, bool]] = []

    def get(self, key: str) -> bool | None:
        assert isinstance(key, str)
        self.reads.append(key)
        return self.values.get(key)

    def put(self, key: str, value: bool) -> None:
        assert isinstance(key, str)
        assert type(value) is bool
        self.writes.append((key, value))
        self.values[key] = value


class _DedupRuntimeContextFixture:
    def __init__(self) -> None:
        self.metrics = _MetricGroupFixture()
        self.map_state = _MapStateFixture()
        self.descriptor = None

    def get_metrics_group(self) -> _MetricGroupFixture:
        return self.metrics

    def get_map_state(self, descriptor):
        self.descriptor = descriptor
        return self.map_state


def _row(
    location_id: str = "QA-RJ",
    event_timestamp: str = "2026-10-09T12:00:00Z",
    ingestion_timestamp: str = "2026-10-09T12:00:01Z",
    temperature: float = 27.4,
) -> tuple[object, ...]:
    return (
        location_id,
        "QA Rio",
        -22.9,
        -43.17,
        temperature,
        18.2,
        42.1,
        58.3,
        7000.0,
        61,
        900,
        event_timestamp,
        ingestion_timestamp,
        "open-meteo",
        1,
    )


@pytest.mark.unit
def test_deduplicator_uses_map_state_with_real_processing_time_ttl_configuration():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from pyflink.datastream.state import StateTtlConfig

    from climate_logistics.flink_job import _DeduplicateWeather

    context = _DedupRuntimeContextFixture()
    operator = _DeduplicateWeather(ttl_seconds=10)
    operator.open(context)

    descriptor = context.descriptor
    assert descriptor.get_name() == "weather_seen_event_times"
    assert str(descriptor.type_info) == "MapTypeInfo<String, Boolean>"
    ttl = descriptor._ttl_config
    assert ttl._ttl.to_milliseconds() == 10_000
    assert ttl._update_type is StateTtlConfig.UpdateType.OnCreateAndWrite
    assert ttl._state_visibility is StateTtlConfig.StateVisibility.NeverReturnExpired
    cleanup = ttl._cleanup_strategies.get_incremental_cleanup_strategy()
    assert cleanup.get_cleanup_size() == 10
    assert cleanup.run_cleanup_for_every_record()


@pytest.mark.unit
def test_duplicate_reads_without_refresh_and_distinct_event_times_are_emitted_once():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from climate_logistics.flink_job import _DeduplicateWeather

    context = _DedupRuntimeContextFixture()
    operator = _DeduplicateWeather(ttl_seconds=10)
    operator.open(context)
    first = _row()
    revised_duplicate = _row(
        ingestion_timestamp="2026-10-09T12:00:09Z",
        temperature=18.0,
    )
    next_identity = _row(event_timestamp="2026-10-09T12:01:00Z")

    emitted = [
        *operator.process_element(first, None),
        *operator.process_element(revised_duplicate, None),
        *operator.process_element(next_identity, None),
    ]

    assert emitted == [first, next_identity]
    assert context.map_state.reads == [
        "2026-10-09T12:00:00Z",
        "2026-10-09T12:00:00Z",
        "2026-10-09T12:01:00Z",
    ]
    assert context.map_state.writes == [
        ("2026-10-09T12:00:00Z", True),
        ("2026-10-09T12:01:00Z", True),
    ]
    counters = context.metrics.groups["data_quality"].counters
    assert counters["accepted_records"].value == 2
    assert counters["duplicate_records"].value == 1


@pytest.mark.unit
def test_same_event_timestamp_is_independent_for_different_keyed_locations():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from climate_logistics.flink_job import _DeduplicateWeather

    rio_context = _DedupRuntimeContextFixture()
    sao_paulo_context = _DedupRuntimeContextFixture()
    rio = _DeduplicateWeather(ttl_seconds=10)
    sao_paulo = _DeduplicateWeather(ttl_seconds=10)
    rio.open(rio_context)
    sao_paulo.open(sao_paulo_context)

    assert list(rio.process_element(_row("QA-RJ"), None)) == [_row("QA-RJ")]
    assert list(sao_paulo.process_element(_row("QA-SP"), None)) == [_row("QA-SP")]
    assert rio_context.map_state.values == {"2026-10-09T12:00:00Z": True}
    assert sao_paulo_context.map_state.values == {"2026-10-09T12:00:00Z": True}


@pytest.mark.unit
def test_qa_ttl_override_requires_matching_exclusive_job_and_group_identity():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from climate_logistics import flink_job

    settings = flink_job.JobSettings(dedup_ttl_seconds=10, qa_audit=True)
    with pytest.raises(ValueError, match="weather-flink-qa"):
        flink_job._validate_execution_identity("weather-kafka-ingestion", settings)
    with pytest.raises(ValueError, match="weather-flink-qa"):
        flink_job._validate_execution_identity(
            "weather-flink-qa-abc", settings, group_id="weather-flink-qa-other-group"
        )

    assert flink_job._validate_execution_identity(
        "weather-flink-qa-abc",
        settings,
        group_id="weather-flink-qa-abc-group",
    ) is None


@pytest.mark.unit
def test_qa_bundle_time_is_a_numeric_millisecond_option_in_real_java_configuration():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from pyflink.java_gateway import get_gateway

    from climate_logistics.flink_job import JobSettings, build_pipeline

    environment = build_pipeline(
        JobSettings(qa_audit=True, group_id="weather-flink-qa-bundle-group")
    )
    option = get_gateway().jvm.org.apache.flink.python.PythonOptions.MAX_BUNDLE_TIME_MILLS
    configured_time = environment._j_stream_execution_environment.getConfiguration().get(option)

    assert configured_time == 1000


@pytest.mark.unit
def test_state_read_cache_is_disabled_for_operational_and_qa_pipelines():
    pytest.importorskip("pyflink", reason="PyFlink tests run in the Flink runtime image")
    from pyflink.java_gateway import get_gateway

    from climate_logistics.flink_job import JobSettings, build_pipeline

    state_cache_option = get_gateway().jvm.org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE
    settings_list = (
        JobSettings(),
        JobSettings(qa_audit=True, group_id="weather-flink-qa-cache-group"),
    )
    for settings in settings_list:
        environment = build_pipeline(settings)
        state_cache_size = environment._j_stream_execution_environment.getConfiguration().get(
            state_cache_option
        )

        assert state_cache_size == 0
