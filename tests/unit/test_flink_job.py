from __future__ import annotations

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