"""PyFlink adapters for event-time weather metrics."""

from __future__ import annotations

import json

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import DataStream
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
    SinkFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import SlidingEventTimeWindows

from climate_logistics.temporal_metrics import (
    MetricAccumulator,
    add_observation,
    empty_accumulator,
    merge_accumulators,
    timestamp_millis,
    window_metric,
)
from climate_logistics.weather_parser import WEATHER_FIELDS

_LOCATION_INDEX = WEATHER_FIELDS.index("location_id")
_EVENT_TIMESTAMP_INDEX = WEATHER_FIELDS.index("event_timestamp")


class WeatherTimestampAssigner(TimestampAssigner):
    """Extract event time from the validated row's event_timestamp field."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return timestamp_millis(str(value[_EVENT_TIMESTAMP_INDEX]))


class WeatherWindowAggregate(AggregateFunction):
    """Apply the pure scalar accumulator through Flink's incremental API."""

    def create_accumulator(self) -> MetricAccumulator:
        return empty_accumulator()

    def add(self, value, accumulator: MetricAccumulator) -> MetricAccumulator:
        return add_observation(accumulator, tuple(value))

    def get_result(self, accumulator: MetricAccumulator) -> MetricAccumulator:
        return accumulator

    def merge(
        self, left: MetricAccumulator, right: MetricAccumulator
    ) -> MetricAccumulator:
        return merge_accumulators(left, right)


class WeatherWindowResult(ProcessWindowFunction):
    """Serialize one aggregate and its window boundaries as a metric record."""

    def process(self, key, context, elements):
        accumulator = next(iter(elements), None)
        if accumulator is None:
            return

        window = context.window()
        metric = window_metric(
            accumulator,
            location_id=str(key),
            start_ms=window.start,
            end_ms=window.end,
        )
        yield json.dumps(
            metric,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )


class CountLateWeather(MapFunction):
    """Count fully late window records without retaining or logging their rows."""

    def __init__(self) -> None:
        self._late_records = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("temporal_stream")
        self._late_records = metrics.counter("late_records")

    def map(self, value) -> bool:
        self._late_records.inc()
        return True


def attach_temporal_metrics(stream: DataStream) -> DataStream:
    """Attach event-time windows and return their deterministic JSON records."""
    late_tag = OutputTag("weather-temporal-late", stream.get_type())
    event_time_stream = stream.assign_timestamps_and_watermarks(
        _weather_watermark_strategy()
    ).name("weather-event-time").uid("weather-event-time")
    windowed_stream = (
        event_time_stream.key_by(
            lambda row: row[_LOCATION_INDEX],
            key_type=Types.STRING(),
        )
        .window(
            SlidingEventTimeWindows.of(
                Time.milliseconds(1_800_000),
                Time.milliseconds(300_000),
            )
        )
        .allowed_lateness(0)
        .side_output_late_data(late_tag)
    )
    metrics_stream = windowed_stream.aggregate(
        WeatherWindowAggregate(),
        WeatherWindowResult(),
        accumulator_type=Types.PICKLED_BYTE_ARRAY(),
        output_type=Types.STRING(),
    ).name("weather-temporal-windows").uid("weather-temporal-windows")

    late_counter_stream = metrics_stream.get_side_output(late_tag).map(
        CountLateWeather(),
        output_type=Types.BOOLEAN(),
    ).name("weather-late-counter").uid("weather-late-counter")
    late_counter_stream.add_sink(
        SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
    ).name("weather-late-discard").uid("weather-late-discard")
    return metrics_stream


def _weather_watermark_strategy() -> WatermarkStrategy:
    """Build the approved bounded-out-of-orderness strategy for weather rows."""
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(120))
        .with_idleness(Duration.of_seconds(60))
        .with_timestamp_assigner(WeatherTimestampAssigner())
    )
