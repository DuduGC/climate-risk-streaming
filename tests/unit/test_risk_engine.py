from __future__ import annotations

import copy
import importlib
import math
from dataclasses import FrozenInstanceError
from typing import Any

import pytest


def _risk_engine():
    try:
        return importlib.import_module("climate_logistics.risk_engine")
    except ModuleNotFoundError as exc:
        if exc.name == "climate_logistics.risk_engine":
            pytest.fail("the pure risk engine has not been implemented yet", pytrace=False)
        raise


def _inputs(**overrides: object):
    values: dict[str, object] = {
        "precipitation_rate_max_mm_per_hour": 0,
        "wind_speed_max": 0,
        "wind_gust_max": 0,
        "visibility_min": 10_000,
    }
    values.update(overrides)
    return _risk_engine().RiskInputs(**values)


def _metric(**overrides: object) -> dict[str, object]:
    metric: dict[str, object] = {
        "component": "flink",
        "operation": "window_aggregate",
        "schema_version": 1,
        "location_id": "RJ01",
        "window_start_ms": 1_791_633_600_000,
        "window_end_ms": 1_791_635_400_000,
        "event_count": 1,
        "temperature_avg": 20.0,
        "precipitation_max": 0.0,
        "precipitation_rate_max_mm_per_hour": 0.0,
        "wind_speed_max": 5.0,
        "wind_gust_max": 10.0,
        "visibility_min": 10_000.0,
        "measurement_interval_min_seconds": 900,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_700_000,
        "last_event_timestamp_ms": 1_791_633_700_000,
    }
    metric.update(overrides)
    return metric


_METRIC_FIELDS = {
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
}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "threshold", "below_score", "at_score", "above_score"),
    [
        ("precipitation_rate_max_mm_per_hour", 2.0, 0, 25, 25),
        ("precipitation_rate_max_mm_per_hour", 20.0, 25, 50, 50),
        ("precipitation_rate_max_mm_per_hour", 30.0, 50, 75, 75),
        ("precipitation_rate_max_mm_per_hour", 60.0, 75, 100, 100),
        ("wind_speed_max", 20.0, 0, 25, 25),
        ("wind_speed_max", 40.0, 25, 50, 50),
        ("wind_speed_max", 60.0, 50, 75, 75),
        ("wind_speed_max", 100.0, 75, 100, 100),
        ("wind_gust_max", 30.0, 0, 25, 25),
        ("wind_gust_max", 50.0, 25, 50, 50),
        ("wind_gust_max", 80.0, 50, 75, 75),
        ("wind_gust_max", 120.0, 75, 100, 100),
    ],
)
def test_greater_equal_thresholds_use_exact_float_boundaries(
    field: str,
    threshold: float,
    below_score: int,
    at_score: int,
    above_score: int,
):
    engine = _risk_engine()
    for value, expected in (
        (math.nextafter(threshold, -math.inf), below_score),
        (threshold, at_score),
        (math.nextafter(threshold, math.inf), above_score),
    ):
        assessment = engine.calculate_risk(_inputs(**{field: value}))
        component = {
            "precipitation_rate_max_mm_per_hour": "precipitation_rate_score",
            "wind_speed_max": "wind_speed_score",
            "wind_gust_max": "wind_gust_score",
        }[field]
        assert getattr(assessment, component) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("threshold", "below_score", "at_score", "above_score"),
    [
        (5_000.0, 25, 0, 0),
        (2_000.0, 50, 25, 25),
        (1_000.0, 75, 50, 50),
        (200.0, 100, 75, 75),
    ],
)
def test_visibility_thresholds_are_strictly_less_than(
    threshold: float,
    below_score: int,
    at_score: int,
    above_score: int,
):
    engine = _risk_engine()
    for value, expected in (
        (math.nextafter(threshold, -math.inf), below_score),
        (threshold, at_score),
        (math.nextafter(threshold, math.inf), above_score),
    ):
        assessment = engine.calculate_risk(_inputs(visibility_min=value))
        assert assessment.visibility_score == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("values", "score", "level"),
    [
        ({}, 0, "LOW"),
        ({"precipitation_rate_max_mm_per_hour": 2}, 25, "MEDIUM"),
        ({"wind_speed_max": 40}, 50, "HIGH"),
        ({"wind_gust_max": 80}, 75, "CRITICAL"),
        ({"visibility_min": 199}, 100, "CRITICAL"),
        ({"visibility_min": 200}, 75, "CRITICAL"),
        ({"visibility_min": 1_000}, 50, "HIGH"),
        ({"wind_speed_max": 40, "wind_gust_max": 50}, 50, "HIGH"),
        (
            {
                "precipitation_rate_max_mm_per_hour": 2,
                "wind_speed_max": 40,
            },
            50,
            "HIGH",
        ),
        ({"wind_gust_max": 120}, 100, "CRITICAL"),
    ],
)
def test_risk_score_is_maximum_component_and_classification_matches(
    values: dict[str, object], score: int, level: str
):
    assessment = _risk_engine().calculate_risk(_inputs(**values))

    assert assessment.risk_score == score
    assert assessment.risk_level.value == level


@pytest.mark.unit
@pytest.mark.parametrize(
    ("score", "level"),
    [
        (0, "LOW"),
        (24, "LOW"),
        (25, "MEDIUM"),
        (49, "MEDIUM"),
        (50, "HIGH"),
        (74, "HIGH"),
        (75, "CRITICAL"),
        (99, "CRITICAL"),
        (100, "CRITICAL"),
    ],
)
def test_classify_score_accepts_every_band_boundary(score: int, level: str):
    assert _risk_engine().classify_score(score).value == level


@pytest.mark.unit
@pytest.mark.parametrize("score", [-1, 101, True, 25.0, "25", None])
def test_classify_score_rejects_values_outside_integer_domain(score: object):
    engine = _risk_engine()
    with pytest.raises(engine.RiskInputError):
        engine.classify_score(score)


@pytest.mark.unit
@pytest.mark.parametrize(
    "invalid",
    [True, None, "0", math.nan, math.inf, -math.inf, -0.01, -1, 10**400],
    ids=[
        "bool",
        "none",
        "string",
        "nan",
        "inf",
        "negative-inf",
        "negative-float",
        "negative-int",
        "overflow-int",
    ],
)
@pytest.mark.parametrize(
    "field",
    [
        "precipitation_rate_max_mm_per_hour",
        "wind_speed_max",
        "wind_gust_max",
        "visibility_min",
    ],
)
def test_risk_inputs_reject_invalid_domain_values_with_controlled_error(
    field: str, invalid: object
):
    engine = _risk_engine()
    values: dict[str, object] = {
        "precipitation_rate_max_mm_per_hour": 0,
        "wind_speed_max": 0,
        "wind_gust_max": 0,
        "visibility_min": 10_000,
    }
    values[field] = invalid

    with pytest.raises(engine.RiskInputError):
        engine.RiskInputs(**values)


@pytest.mark.unit
def test_risk_inputs_normalize_finite_integers_to_float():
    inputs = _inputs(
        precipitation_rate_max_mm_per_hour=2,
        wind_speed_max=40,
        wind_gust_max=80,
        visibility_min=200,
    )

    assert inputs.precipitation_rate_max_mm_per_hour == 2.0
    assert inputs.wind_speed_max == 40.0
    assert inputs.wind_gust_max == 80.0
    assert inputs.visibility_min == 200.0
    assert all(
        type(getattr(inputs, field)) is float
        for field in (
            "precipitation_rate_max_mm_per_hour",
            "wind_speed_max",
            "wind_gust_max",
            "visibility_min",
        )
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "values", "expected"),
    [
        (
            "precipitation_rate_max_mm_per_hour",
            [0, 1.99, 2, 19.99, 20, 29.99, 30, 59.99, 60],
            [0, 0, 25, 25, 50, 50, 75, 75, 100],
        ),
        (
            "wind_speed_max",
            [0, 19.99, 20, 39.99, 40, 59.99, 60, 99.99, 100],
            [0, 0, 25, 25, 50, 50, 75, 75, 100],
        ),
        (
            "wind_gust_max",
            [0, 29.99, 30, 49.99, 50, 79.99, 80, 119.99, 120],
            [0, 0, 25, 25, 50, 50, 75, 75, 100],
        ),
        (
            "visibility_min",
            [10_000, 5_000, 4_999, 2_000, 1_999, 1_000, 999, 200, 199],
            [0, 0, 25, 25, 50, 50, 75, 75, 100],
        ),
    ],
)
def test_each_component_is_monotonic_in_its_physical_direction(
    field: str, values: list[float], expected: list[int]
):
    engine = _risk_engine()
    observed = [
        getattr(engine.calculate_risk(_inputs(**{field: value})), {
            "precipitation_rate_max_mm_per_hour": "precipitation_rate_score",
            "wind_speed_max": "wind_speed_score",
            "wind_gust_max": "wind_gust_score",
            "visibility_min": "visibility_score",
        }[field])
        for value in values
    ]

    assert observed == expected


@pytest.mark.unit
def test_assessment_and_inputs_are_frozen_and_components_are_returned_as_fresh_dicts():
    engine = _risk_engine()
    inputs = _inputs(precipitation_rate_max_mm_per_hour=2)
    assessment = engine.calculate_risk(inputs)
    original_components = {
        "precipitation_rate": 25,
        "wind_speed": 0,
        "wind_gust": 0,
        "visibility": 0,
    }

    assert assessment.components_dict() == original_components
    first = assessment.components_dict()
    first["precipitation_rate"] = 100
    first["new_factor"] = 75
    assert assessment.components_dict() == original_components
    assert assessment.risk_score == 25
    assert assessment.rules_version == "mvp-1"
    with pytest.raises(FrozenInstanceError):
        inputs.wind_speed_max = 100
    with pytest.raises(FrozenInstanceError):
        assessment.wind_speed_score = 100


@pytest.mark.unit
def test_metric_helpers_validate_the_exact_window_aggregate_contract_and_copy_it():
    engine = _risk_engine()
    metric = _metric(
        precipitation_rate_max_mm_per_hour=2,
        wind_speed_max=40,
        wind_gust_max=50,
        visibility_min=6_000,
    )
    original = copy.deepcopy(metric)

    inputs = engine.inputs_from_metric(metric)
    enriched = engine.enrich_metric(metric)

    assert inputs.precipitation_rate_max_mm_per_hour == 2.0
    assert inputs.wind_speed_max == 40.0
    assert inputs.wind_gust_max == 50.0
    assert inputs.visibility_min == 6_000.0
    assert metric == original
    assert enriched is not metric
    assert set(enriched) == _METRIC_FIELDS | {
        "risk_score",
        "risk_level",
        "risk_components",
        "risk_rules_version",
    }
    assert len(enriched) == 21
    assert {key: enriched[key] for key in _METRIC_FIELDS - {"operation"}} == {
        key: original[key] for key in _METRIC_FIELDS - {"operation"}
    }
    assert enriched["operation"] == "risk_assessment"
    assert enriched["risk_score"] == 50
    assert enriched["risk_level"] == "HIGH"
    assert enriched["risk_components"] == {
        "precipitation_rate": 25,
        "wind_speed": 50,
        "wind_gust": 50,
        "visibility": 0,
    }
    assert enriched["risk_rules_version"] == "mvp-1"

    enriched["risk_components"]["wind_speed"] = 100
    assert metric == original
    assert engine.enrich_metric(metric)["risk_components"]["wind_speed"] == 50


@pytest.mark.unit
def test_metric_contract_allows_negative_epoch_aligned_window_start():
    metric = _metric(
        window_start_ms=-300_000,
        window_end_ms=1_500_000,
        first_event_timestamp_ms=0,
        last_event_timestamp_ms=0,
    )

    enriched = _risk_engine().enrich_metric(metric)

    assert enriched["window_start_ms"] == -300_000
    assert enriched["window_end_ms"] == 1_500_000


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"component": "other"},
        {"operation": "risk_assessment"},
        {"operation": "other"},
        {"schema_version": True},
        {"schema_version": 2},
        {"location_id": ""},
        {"event_count": 0},
        {"event_count": True},
        {"event_count": 1.0},
        {"window_start_ms": True},
        {"window_end_ms": 1_791_633_600_000},
        {"first_event_timestamp_ms": 1_791_633_599_999},
        {"last_event_timestamp_ms": 1_791_635_400_000},
        {"first_event_timestamp_ms": True},
        {"last_event_timestamp_ms": 1_791_633_699_999},
        {
            "first_event_timestamp_ms": 1_791_633_800_000,
            "last_event_timestamp_ms": 1_791_633_700_000,
        },
    ],
)
def test_metric_helpers_reject_invalid_envelope_and_window_metadata(
    overrides: dict[str, object]
):
    engine = _risk_engine()

    with pytest.raises(engine.RiskInputError):
        engine.enrich_metric(_metric(**overrides))


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate",
    [
        lambda metric: metric.pop("wind_gust_max"),
        lambda metric: metric.update(extra_metric_field=1),
        lambda metric: metric.update(precipitation_rate_max_mm_per_hour=None),
        lambda metric: metric.update(wind_speed_max=True),
        lambda metric: metric.update(wind_gust_max=math.nan),
        lambda metric: metric.update(visibility_min=math.inf),
        lambda metric: metric.update(precipitation_rate_max_mm_per_hour=-1),
    ],
)
def test_metric_helpers_reject_missing_unknown_or_invalid_factor_fields(mutate):
    engine = _risk_engine()
    metric = _metric()
    mutate(metric)

    with pytest.raises(engine.RiskInputError):
        engine.inputs_from_metric(metric)


@pytest.mark.unit
@pytest.mark.parametrize("metric", [[], "not-an-object", None])
def test_metric_helpers_reject_non_mapping_input(metric: Any):
    engine = _risk_engine()

    with pytest.raises(engine.RiskInputError):
        engine.enrich_metric(metric)
