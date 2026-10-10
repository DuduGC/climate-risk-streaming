from __future__ import annotations

from dataclasses import fields

import pytest

from climate_logistics.temporal_metrics import (
    MetricAccumulator,
    add_observation,
    empty_accumulator,
    merge_accumulators,
    timestamp_millis,
    window_metric,
)


def _observation(
    *,
    temperature: float,
    precipitation: float,
    interval: int,
    wind_speed: float,
    wind_gust: float,
    visibility: float,
    timestamp: str,
    location_id: str = "RJ01",
) -> list[object]:
    return [
        location_id,
        "Rio de Janeiro",
        -22.9,
        -43.2,
        temperature,
        precipitation,
        wind_speed,
        wind_gust,
        visibility,
        61,
        interval,
        timestamp,
        "2026-10-10T12:01:00Z",
        "open-meteo",
        1,
    ]


@pytest.mark.unit
def test_window_metric_aggregates_observations_without_mutating_rows():
    first = _observation(
        temperature=20,
        precipitation=2,
        interval=900,
        wind_speed=40,
        wind_gust=55,
        visibility=9000,
        timestamp="2026-10-10T12:00:00Z",
    )
    second = _observation(
        temperature=30,
        precipitation=1,
        interval=300,
        wind_speed=50,
        wind_gust=60,
        visibility=6000,
        timestamp="2026-10-10T12:05:00Z",
    )
    original_rows = (first.copy(), second.copy())
    accumulator = add_observation(add_observation(empty_accumulator(), first), second)

    metric = window_metric(
        accumulator,
        location_id="RJ01",
        start_ms=1_791_633_600_000,
        end_ms=1_791_635_400_000,
    )

    expected = {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": "RJ01",
        "window_start_ms": 1_791_633_600_000,
        "window_end_ms": 1_791_635_400_000,
        "event_count": 2,
        "temperature_avg": 25.0,
        "precipitation_max": 2,
        "precipitation_rate_max_mm_per_hour": 12.0,
        "wind_speed_max": 50,
        "wind_gust_max": 60,
        "visibility_min": 6000,
        "measurement_interval_min_seconds": 300,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_600_000,
        "last_event_timestamp_ms": 1_791_633_900_000,
    }
    assert metric.keys() == expected.keys()
    assert metric["temperature_avg"] == pytest.approx(25.0)
    assert metric["precipitation_rate_max_mm_per_hour"] == pytest.approx(12.0)
    for field in expected.keys() - {
        "temperature_avg",
        "precipitation_rate_max_mm_per_hour",
    }:
        assert metric[field] == expected[field]
    assert first == original_rows[0]
    assert second == original_rows[1]
    assert len(metric) == 17
    assert "temperature_sum" not in metric


@pytest.mark.unit
def test_merging_accumulators_preserves_extrema_and_empty_identity():
    first = _observation(
        temperature=20,
        precipitation=2,
        interval=900,
        wind_speed=40,
        wind_gust=55,
        visibility=9000,
        timestamp="2026-10-10T12:00:00Z",
    )
    second = _observation(
        temperature=30,
        precipitation=1,
        interval=300,
        wind_speed=50,
        wind_gust=60,
        visibility=6000,
        timestamp="2026-10-10T12:05:00Z",
    )
    empty = empty_accumulator()
    left = add_observation(empty_accumulator(), first)
    right = add_observation(empty_accumulator(), second)

    assert merge_accumulators(empty, left) == left
    assert merge_accumulators(right, empty) == right
    merged = merge_accumulators(left, right)
    sequential = add_observation(left, second)
    assert merged == sequential
    assert merge_accumulators(empty, empty) == empty


@pytest.mark.unit
def test_empty_accumulator_has_eleven_scalar_fields_and_cannot_be_finalized():
    empty = empty_accumulator()

    assert len(fields(MetricAccumulator)) == 11
    assert empty.event_count == 0
    assert empty.temperature_sum == 0
    assert all(
        getattr(empty, field.name) is None
        for field in fields(MetricAccumulator)
        if field.name not in {"event_count", "temperature_sum"}
    )
    assert all(
        not isinstance(getattr(empty, field.name), list | dict | set)
        for field in fields(MetricAccumulator)
    )
    with pytest.raises(ValueError, match="empty"):
        window_metric(empty, location_id="RJ01", start_ms=0, end_ms=1)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T03:00:00+03:00", 0),
        ("1970-01-01T00:00:00.123999Z", 123),
        ("9999-12-31T23:59:59.999999Z", 253_402_300_799_999),
    ],
)
def test_timestamp_millis_uses_utc_integer_arithmetic(timestamp: str, expected: int):
    assert timestamp_millis(timestamp) == expected


@pytest.mark.unit
def test_timestamp_millis_rejects_naive_iso_timestamp():
    with pytest.raises(ValueError, match="timezone|fuso|aware"):
        timestamp_millis("2026-10-10T12:00:00")
