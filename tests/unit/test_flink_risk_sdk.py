from __future__ import annotations

import importlib
import json
import math

import pytest

pytest.importorskip("pyflink", reason="PyFlink SDK tests run in the Flink runtime image")

from pyflink.common import Types  # noqa: E402
from pyflink.datastream import StreamExecutionEnvironment  # noqa: E402


def _flink_risk():
    try:
        return importlib.import_module("climate_logistics.flink_risk")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.flink_risk":
            pytest.fail("the PyFlink risk adapter has not been implemented yet", pytrace=False)
        raise


def _metric(**overrides: object) -> dict[str, object]:
    metric: dict[str, object] = {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": "QA-RISK",
        "window_start_ms": 1_791_633_600_000,
        "window_end_ms": 1_791_635_400_000,
        "event_count": 1,
        "temperature_avg": 20.0,
        "precipitation_max": 0.0,
        "precipitation_rate_max_mm_per_hour": 2.0,
        "wind_speed_max": 40.0,
        "wind_gust_max": 50.0,
        "visibility_min": 6_000.0,
        "measurement_interval_min_seconds": 900,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_700_000,
        "last_event_timestamp_ms": 1_791_633_700_000,
    }
    metric.update(overrides)
    return metric


class _Counter:
    def __init__(self) -> None:
        self.value = 0

    def inc(self) -> None:
        self.value += 1


class _Group:
    def __init__(self, groups: dict[str, _Group] | None = None) -> None:
        self.groups = {} if groups is None else groups
        self.counters: dict[str, _Counter] = {}

    def add_group(self, name: str) -> _Group:
        group = self.groups.setdefault(name, _Group(self.groups))
        return group

    def counter(self, name: str) -> _Counter:
        return self.counters.setdefault(name, _Counter())


class _RuntimeContext:
    def __init__(self) -> None:
        self.metrics = _Group()

    def get_metrics_group(self) -> _Group:
        return self.metrics


@pytest.mark.unit
def test_risk_map_serializes_full_assessment_and_counts_only_after_valid_output():
    function = _flink_risk().AssessWindowRisk()
    context = _RuntimeContext()
    function.open(context)

    serialized = function.map(json.dumps(_metric(), separators=(",", ":")))
    output = json.loads(serialized)

    assert isinstance(serialized, str)
    assert set(output) == set(_metric()) | {
        "risk_score",
        "risk_level",
        "risk_components",
        "risk_rules_version",
    }
    assert output["operation"] == "risk_assessment"
    assert output["risk_score"] == 50
    assert output["risk_level"] == "HIGH"
    assert output["risk_components"] == {
        "precipitation_rate": 25,
        "wind_speed": 50,
        "wind_gust": 50,
        "visibility": 0,
    }
    assert output["risk_rules_version"] == "mvp-1"
    assert context.metrics.groups["risk_engine"].counters["assessed_windows"].value == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw",
    [
        "{invalid-json",
        json.dumps([1, 2, 3]),
        json.dumps(_metric(operation="risk_assessment")),
        json.dumps(_metric(visibility_min=None)),
        json.dumps(_metric(precipitation_rate_max_mm_per_hour=math.nan)),
        json.dumps(_metric(temperature_avg=math.nan)),
    ],
)
def test_risk_map_propagates_invalid_contract_or_serialization_without_counting(raw: str):
    function = _flink_risk().AssessWindowRisk()
    context = _RuntimeContext()
    function.open(context)

    with pytest.raises((ValueError, TypeError)):
        function.map(raw)

    assert context.metrics.groups["risk_engine"].counters["assessed_windows"].value == 0


@pytest.mark.unit
def test_attach_risk_assessment_returns_a_string_stream_with_its_own_uid():
    env = StreamExecutionEnvironment.get_execution_environment()
    stream = env.from_collection([json.dumps(_metric())], type_info=Types.STRING())

    risk = _flink_risk().attach_risk_assessment(stream)
    risk.print("weather-risk").name("weather-risk").uid("weather-risk")

    assert risk._j_data_stream.getTransformation().getOutputType().toString() == str(
        Types.STRING()
    )
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )
    assert "weather-risk-assessment" in descriptions
    assert "weather-risk" in descriptions


@pytest.mark.unit
def test_build_pipeline_keeps_three_diagnostic_sinks_and_all_existing_stateful_uids():
    from climate_logistics.flink_job import JobSettings, build_pipeline

    env = build_pipeline(JobSettings())
    plan = json.loads(env.get_execution_plan())
    descriptions = " ".join(
        " ".join(str(value) for value in node.values()) for node in plan["nodes"]
    )

    for uid in (
        "weather-raw-source",
        "weather-json-parser",
        "weather-event-deduplication",
        "weather-parse-diagnostic",
        "weather-event-time",
        "weather-temporal-windows",
        "weather-late-counter",
        "weather-late-discard",
        "weather-metrics",
        "weather-risk-assessment",
        "weather-risk",
        "weather-ingestion",
    ):
        assert uid in descriptions

    sink_names = {"weather-ingestion", "weather-metrics", "weather-risk"}
    assert sink_names <= set(descriptions.split())
