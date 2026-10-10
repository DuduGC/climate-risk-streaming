from __future__ import annotations

import json
from datetime import timedelta

import pytest

pytest.importorskip("pyflink", reason="PyFlink SDK tests run in the Flink runtime image")

from pyflink.common import Row, Time, Types  # noqa: E402
from pyflink.common.restart_strategy import RestartStrategies  # noqa: E402
from pyflink.datastream import StreamExecutionEnvironment  # noqa: E402
from pyflink.datastream.checkpointing_mode import CheckpointingMode  # noqa: E402
from pyflink.datastream.data_stream import WindowedStream  # noqa: E402
from pyflink.datastream.window import SlidingEventTimeWindows  # noqa: E402
from pyflink.java_gateway import get_gateway  # noqa: E402

from climate_logistics.flink_job import (  # noqa: E402
    DEFAULT_JOB_NAME,
    JobSettings,
    _arguments,
    _row_type,
    _validate_execution_identity,
    build_pipeline,
)
from climate_logistics.flink_temporal import (  # noqa: E402
    CountLateWeather,
    WeatherTimestampAssigner,
    WeatherWindowAggregate,
    WeatherWindowResult,
    _weather_watermark_strategy,
    attach_temporal_metrics,
    configure_checkpointing,
)


def _row(
    *, location_id: str = "RJ01", timestamp: str = "2026-10-10T12:00:00Z"
) -> Row:
    return Row(
        location_id,
        "Rio de Janeiro",
        -22.9,
        -43.2,
        25.0,
        2.0,
        40.0,
        55.0,
        9000.0,
        61,
        900,
        timestamp,
        "2026-10-10T12:01:00Z",
        "open-meteo",
        1,
    )


def test_timestamp_assigner_uses_event_timestamp_as_utc_epoch_milliseconds():
    row = _row(timestamp="2026-10-10T15:00:00.123999+03:00")

    assert WeatherTimestampAssigner().extract_timestamp(row, 99) == 1_791_633_600_123


def test_watermark_strategy_uses_two_minute_disorder_and_one_minute_idleness():
    strategy = _weather_watermark_strategy()._j_watermark_strategy

    idleness_field = strategy.getClass().getDeclaredField("idlenessTimeout")
    idleness_field.setAccessible(True)
    assert idleness_field.get(strategy).toMillis() == 60_000

    base_field = strategy.getClass().getDeclaredField("baseStrategy")
    base_field.setAccessible(True)
    generator = base_field.get(strategy).createWatermarkGenerator(None)
    disorder_field = generator.getClass().getDeclaredField("outOfOrdernessMillis")
    disorder_field.setAccessible(True)
    assert disorder_field.getLong(generator) == 120_000


def test_watermark_strategy_keeps_event_timestamp_assigner_after_idleness():
    assigner = _weather_watermark_strategy()._timestamp_assigner

    assert isinstance(assigner, WeatherTimestampAssigner)
    row = _row(timestamp="2026-10-10T15:00:00.123999+03:00")
    assert assigner.extract_timestamp(row, 99) == 1_791_633_600_123


def test_real_sliding_window_assigner_uses_six_epoch_aligned_half_open_windows():
    assigner = SlidingEventTimeWindows.of(Time.minutes(30), Time.minutes(5))

    windows_at_zero = sorted(
        (window.start, window.end)
        for window in assigner.assign_windows(None, 0, None)
    )
    windows_at_five_minutes = sorted(
        (window.start, window.end)
        for window in assigner.assign_windows(None, 300_000, None)
    )

    assert windows_at_zero == [
        (-1_500_000, 300_000),
        (-1_200_000, 600_000),
        (-900_000, 900_000),
        (-600_000, 1_200_000),
        (-300_000, 1_500_000),
        (0, 1_800_000),
    ]
    assert windows_at_five_minutes == [
        (-1_200_000, 600_000),
        (-900_000, 900_000),
        (-600_000, 1_200_000),
        (-300_000, 1_500_000),
        (0, 1_800_000),
        (300_000, 2_100_000),
    ]


def test_attach_passes_integer_zero_allowed_lateness_to_the_real_sdk(monkeypatch):
    original_allowed_lateness = WindowedStream.allowed_lateness
    observed_values: list[int] = []

    def strict_integer_milliseconds(windowed_stream, milliseconds):
        assert type(milliseconds) is int, "PyFlink allowed_lateness expects integer milliseconds"
        observed_values.append(milliseconds)
        return original_allowed_lateness(windowed_stream, milliseconds)

    monkeypatch.setattr(WindowedStream, "allowed_lateness", strict_integer_milliseconds)
    env = StreamExecutionEnvironment.get_execution_environment()
    stream = env.from_collection([_row()], type_info=_row_type())

    metrics = attach_temporal_metrics(stream)

    metrics.print("weather-metrics").name("weather-metrics").uid("weather-metrics")
    assert observed_values == [0]
    json.loads(env.get_execution_plan())


def test_window_aggregate_and_result_adapt_the_pure_contract_to_sdk_callbacks():
    aggregate = WeatherWindowAggregate()
    accumulator = aggregate.create_accumulator()
    row = _row()
    accumulator = aggregate.add(row, accumulator)
    assert aggregate.get_result(accumulator) == accumulator

    class _Window:
        start = 1_791_633_600_000
        end = 1_791_635_400_000

    class _Context:
        @staticmethod
        def window() -> _Window:
            return _Window()

    serialized = list(WeatherWindowResult().process("RJ01", _Context(), [accumulator]))
    metric = json.loads(serialized[0])
    assert metric["location_id"] == "RJ01"
    assert metric["event_count"] == 1
    assert metric["temperature_avg"] == 25.0
    assert metric["window_start_ms"] == 1_791_633_600_000
    assert metric["window_end_ms"] == 1_791_635_400_000
    assert len(serialized) == 1


def test_late_counter_counts_an_event_and_returns_only_a_boolean():
    class _Counter:
        value = 0

        def inc(self) -> None:
            self.value += 1

    class _Group:
        def __init__(self) -> None:
            self.counters: dict[str, _Counter] = {}

        def add_group(self, name: str) -> _Group:
            assert name == "temporal_stream"
            return self

        def counter(self, name: str) -> _Counter:
            return self.counters.setdefault(name, _Counter())

    class _Context:
        metrics = _Group()

        def get_metrics_group(self) -> _Group:
            return self.metrics

    context = _Context()
    counter = CountLateWeather()
    counter.open(context)

    result = counter.map(_row())

    assert type(result) is bool
    assert result is True
    assert context.metrics.counters["late_records"].value == 1


def test_attach_temporal_metrics_builds_a_serializable_sdk_graph():
    env = StreamExecutionEnvironment.get_execution_environment()
    stream = env.from_collection([_row()], type_info=_row_type())
    metrics = attach_temporal_metrics(stream)
    metrics.print("weather-metrics").name("weather-metrics").uid("weather-metrics")

    assert metrics._j_data_stream.getTransformation().getOutputType().toString() == str(
        Types.STRING()
    )
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )
    assert "weather-event-time" in descriptions
    assert "weather-temporal-windows" in descriptions
    assert "weather-late-counter" in descriptions
    assert "weather-late-discard" in descriptions
    assert "weather-metrics" in descriptions


def test_build_pipeline_keeps_the_existing_diagnostic_and_adds_temporal_sink():
    env = build_pipeline(JobSettings())

    assert env.get_config().get_auto_watermark_interval() == 200
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )

    assert "weather-ingestion" in descriptions
    assert "weather-metrics" in descriptions


def test_checkpoint_configuration_sets_effective_storage_retention_and_restarts():
    env = build_pipeline(JobSettings())

    checkpoint = env.get_checkpoint_config()
    assert checkpoint.is_checkpointing_enabled()
    assert checkpoint.get_checkpoint_interval() == 60_000
    assert checkpoint.get_checkpointing_mode() is CheckpointingMode.EXACTLY_ONCE
    assert checkpoint.get_checkpoint_timeout() == 120_000
    assert checkpoint.get_min_pause_between_checkpoints() == 10_000
    assert checkpoint.get_max_concurrent_checkpoints() == 1
    assert checkpoint.get_tolerable_checkpoint_failure_number() == 0

    configuration = env._j_stream_execution_environment.getConfiguration()
    assert configuration.getString("execution.checkpointing.storage", "") == "filesystem"
    assert (
        configuration.getString("execution.checkpointing.dir", "")
        == "file:///opt/flink/checkpoints"
    )
    checkpointing_options = get_gateway().jvm.org.apache.flink.configuration.CheckpointingOptions
    externalized_retention = configuration.get(
        checkpointing_options.EXTERNALIZED_CHECKPOINT_RETENTION
    )
    assert externalized_retention.name() == "RETAIN_ON_CANCELLATION"
    assert (
        configuration.get(checkpointing_options.MAX_RETAINED_CHECKPOINTS) == 2
    )
    assert configuration.getInteger("python.state.cache-size", -1) == 0

    restart = env.get_restart_strategy()
    assert isinstance(restart, RestartStrategies.FixedDelayRestartStrategyConfiguration)
    assert restart.get_restart_attempts() == 3
    assert restart.get_delay_between_attempts_interval() == timedelta(seconds=10)


def test_checkpoint_configuration_applies_the_requested_qa_interval():
    env = StreamExecutionEnvironment.get_execution_environment()

    configure_checkpointing(env, interval_seconds=10)

    assert env.get_checkpoint_config().get_checkpoint_interval() == 10_000


@pytest.mark.parametrize("interval", [True, 4, 61, 10.0, "10"])
def test_job_settings_reject_invalid_checkpoint_intervals(interval):
    with pytest.raises(ValueError, match="checkpoint.*inteiro|checkpoint.*5 e 60"):
        JobSettings(checkpoint_interval_seconds=interval)


@pytest.mark.parametrize("interval", [True, 4, 61, 10.0, "10"])
def test_checkpoint_configuration_rejects_invalid_intervals(interval):
    env = StreamExecutionEnvironment.get_execution_environment()

    with pytest.raises(ValueError, match="checkpoint.*inteiro|checkpoint.*5 e 60"):
        configure_checkpointing(env, interval_seconds=interval)


def test_custom_checkpoint_interval_requires_qa_and_matching_job_group():
    with pytest.raises(ValueError, match="somente QA"):
        JobSettings(checkpoint_interval_seconds=10)

    settings = JobSettings(
        group_id="weather-flink-qa-ab12-group",
        qa_audit=True,
        checkpoint_interval_seconds=10,
    )
    with pytest.raises(ValueError, match="weather-flink-qa"):
        _validate_execution_identity(DEFAULT_JOB_NAME, settings)

    with pytest.raises(ValueError, match="weather-flink-qa"):
        _validate_execution_identity(
            "weather-flink-qa-ab12", settings, group_id="weather-flink-qa-other-group"
        )


def test_explicit_default_checkpoint_cli_override_still_requires_qa_identity(monkeypatch):
    from climate_logistics import flink_job

    captured: dict[str, object] = {}

    def fake_execute(job_name, settings):
        captured["job_name"] = job_name
        captured["settings"] = settings
        return "job-id"

    monkeypatch.setattr(flink_job, "execute", fake_execute)

    with pytest.raises(ValueError, match="QA"):
        flink_job.main(["--qa-checkpoint-interval-seconds", "60"])

    with pytest.raises(ValueError, match="weather-flink-qa"):
        flink_job.main(
            ["--qa-checkpoint-interval-seconds", "60", "--qa-audit"]
        )

    assert (
        flink_job.main(
            [
                "--qa-checkpoint-interval-seconds",
                "60",
                "--qa-audit",
                "--job-name",
                "weather-flink-qa-ab12",
                "--group-id",
                "weather-flink-qa-ab12-group",
            ]
        )
        == 0
    )
    settings = captured["settings"]
    assert captured["job_name"] == "weather-flink-qa-ab12"
    assert isinstance(settings, JobSettings)
    assert settings.checkpoint_interval_seconds == 60
    assert _arguments(["--qa-checkpoint-interval-seconds", "5"]).qa_checkpoint_interval_seconds == 5
    with pytest.raises(SystemExit):
        _arguments(["--qa-checkpoint-interval-seconds", "61"])
