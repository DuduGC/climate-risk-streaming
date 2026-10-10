"""Pure incremental aggregation for event-time weather windows."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from climate_logistics.weather_parser import WEATHER_FIELDS

_FIELD_INDEX = {field: index for index, field in enumerate(WEATHER_FIELDS)}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MILLISECONDS_PER_DAY = 86_400_000


@dataclass(frozen=True)
class MetricAccumulator:
    """Fixed-size window state containing only scalar aggregates."""

    event_count: int = 0
    temperature_sum: float = 0.0
    precipitation_max: float | None = None
    precipitation_rate_max_mm_per_hour: float | None = None
    wind_speed_max: float | None = None
    wind_gust_max: float | None = None
    visibility_min: float | None = None
    measurement_interval_min_seconds: int | None = None
    measurement_interval_max_seconds: int | None = None
    first_event_timestamp_ms: int | None = None
    last_event_timestamp_ms: int | None = None


def timestamp_millis(timestamp: str) -> int:
    """Convert an aware ISO-8601 timestamp to UTC epoch milliseconds exactly."""
    if not isinstance(timestamp, str):
        raise ValueError("timestamp must be an ISO-8601 string with a timezone")

    normalized = f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        utc_timestamp = parsed.astimezone(UTC)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("timestamp must be a valid aware ISO-8601 timestamp") from exc

    elapsed = utc_timestamp - _EPOCH
    return (
        elapsed.days * _MILLISECONDS_PER_DAY
        + elapsed.seconds * 1_000
        + elapsed.microseconds // 1_000
    )


def empty_accumulator() -> MetricAccumulator:
    """Return the identity value for weather window aggregation."""
    return MetricAccumulator()


def add_observation(
    accumulator: MetricAccumulator, values: Sequence[object]
) -> MetricAccumulator:
    """Return an accumulator that also includes one validated WEATHER_FIELDS row."""
    if len(values) != len(WEATHER_FIELDS):
        raise ValueError("values do not match WEATHER_FIELDS")

    temperature = values[_FIELD_INDEX["temperature"]]
    precipitation = values[_FIELD_INDEX["precipitation"]]
    interval = values[_FIELD_INDEX["measurement_interval_seconds"]]
    event_timestamp_ms = timestamp_millis(str(values[_FIELD_INDEX["event_timestamp"]]))
    precipitation_rate = precipitation * 3_600 / interval

    return MetricAccumulator(
        event_count=accumulator.event_count + 1,
        temperature_sum=accumulator.temperature_sum + temperature,
        precipitation_max=(
            precipitation
            if accumulator.precipitation_max is None
            else max(accumulator.precipitation_max, precipitation)
        ),
        precipitation_rate_max_mm_per_hour=(
            precipitation_rate
            if accumulator.precipitation_rate_max_mm_per_hour is None
            else max(
                accumulator.precipitation_rate_max_mm_per_hour,
                precipitation_rate,
            )
        ),
        wind_speed_max=(
            values[_FIELD_INDEX["wind_speed"]]
            if accumulator.wind_speed_max is None
            else max(accumulator.wind_speed_max, values[_FIELD_INDEX["wind_speed"]])
        ),
        wind_gust_max=(
            values[_FIELD_INDEX["wind_gust"]]
            if accumulator.wind_gust_max is None
            else max(accumulator.wind_gust_max, values[_FIELD_INDEX["wind_gust"]])
        ),
        visibility_min=(
            values[_FIELD_INDEX["visibility"]]
            if accumulator.visibility_min is None
            else min(accumulator.visibility_min, values[_FIELD_INDEX["visibility"]])
        ),
        measurement_interval_min_seconds=(
            interval
            if accumulator.measurement_interval_min_seconds is None
            else min(accumulator.measurement_interval_min_seconds, interval)
        ),
        measurement_interval_max_seconds=(
            interval
            if accumulator.measurement_interval_max_seconds is None
            else max(accumulator.measurement_interval_max_seconds, interval)
        ),
        first_event_timestamp_ms=(
            event_timestamp_ms
            if accumulator.first_event_timestamp_ms is None
            else min(accumulator.first_event_timestamp_ms, event_timestamp_ms)
        ),
        last_event_timestamp_ms=(
            event_timestamp_ms
            if accumulator.last_event_timestamp_ms is None
            else max(accumulator.last_event_timestamp_ms, event_timestamp_ms)
        ),
    )


def merge_accumulators(
    left: MetricAccumulator, right: MetricAccumulator
) -> MetricAccumulator:
    """Combine two partial aggregates without retaining their source rows."""
    if left.event_count == 0:
        return right
    if right.event_count == 0:
        return left

    return MetricAccumulator(
        event_count=left.event_count + right.event_count,
        temperature_sum=left.temperature_sum + right.temperature_sum,
        precipitation_max=max(left.precipitation_max, right.precipitation_max),
        precipitation_rate_max_mm_per_hour=max(
            left.precipitation_rate_max_mm_per_hour,
            right.precipitation_rate_max_mm_per_hour,
        ),
        wind_speed_max=max(left.wind_speed_max, right.wind_speed_max),
        wind_gust_max=max(left.wind_gust_max, right.wind_gust_max),
        visibility_min=min(left.visibility_min, right.visibility_min),
        measurement_interval_min_seconds=min(
            left.measurement_interval_min_seconds,
            right.measurement_interval_min_seconds,
        ),
        measurement_interval_max_seconds=max(
            left.measurement_interval_max_seconds,
            right.measurement_interval_max_seconds,
        ),
        first_event_timestamp_ms=min(
            left.first_event_timestamp_ms,
            right.first_event_timestamp_ms,
        ),
        last_event_timestamp_ms=max(
            left.last_event_timestamp_ms,
            right.last_event_timestamp_ms,
        ),
    )


def window_metric(
    accumulator: MetricAccumulator,
    *,
    location_id: str,
    start_ms: int,
    end_ms: int,
) -> dict[str, object]:
    """Build the public metric contract for one non-empty window."""
    if accumulator.event_count == 0:
        raise ValueError("cannot finalize an empty window accumulator")

    return {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "event_count": accumulator.event_count,
        "temperature_avg": accumulator.temperature_sum / accumulator.event_count,
        "precipitation_max": accumulator.precipitation_max,
        "precipitation_rate_max_mm_per_hour": (
            accumulator.precipitation_rate_max_mm_per_hour
        ),
        "wind_speed_max": accumulator.wind_speed_max,
        "wind_gust_max": accumulator.wind_gust_max,
        "visibility_min": accumulator.visibility_min,
        "measurement_interval_min_seconds": (
            accumulator.measurement_interval_min_seconds
        ),
        "measurement_interval_max_seconds": (
            accumulator.measurement_interval_max_seconds
        ),
        "first_event_timestamp_ms": accumulator.first_event_timestamp_ms,
        "last_event_timestamp_ms": accumulator.last_event_timestamp_ms,
    }
