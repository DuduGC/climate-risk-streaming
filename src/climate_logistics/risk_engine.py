"""Pure, deterministic assessment of risk from one temporal metric window."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from climate_logistics.temporal_metrics import WINDOW_METRIC_FIELDS

RULES_VERSION = "mvp-1"

_PRECIPITATION_THRESHOLDS: tuple[tuple[float, int], ...] = (
    (2.0, 25),
    (20.0, 50),
    (30.0, 75),
    (60.0, 100),
)
_WIND_SPEED_THRESHOLDS: tuple[tuple[float, int], ...] = (
    (20.0, 25),
    (40.0, 50),
    (60.0, 75),
    (100.0, 100),
)
_WIND_GUST_THRESHOLDS: tuple[tuple[float, int], ...] = (
    (30.0, 25),
    (50.0, 50),
    (80.0, 75),
    (120.0, 100),
)
_VISIBILITY_THRESHOLDS: tuple[tuple[float, int], ...] = (
    (200.0, 100),
    (1_000.0, 75),
    (2_000.0, 50),
    (5_000.0, 25),
)


class RiskInputError(ValueError):
    """Raised when an input does not satisfy the risk engine contract."""


@dataclass(frozen=True, slots=True)
class RiskInputs:
    """The four non-negative finite measurements used by the MVP policy."""

    precipitation_rate_max_mm_per_hour: float
    wind_speed_max: float
    wind_gust_max: float
    visibility_min: float

    def __post_init__(self) -> None:
        for field_name in (
            "precipitation_rate_max_mm_per_hour",
            "wind_speed_max",
            "wind_gust_max",
            "visibility_min",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise RiskInputError(f"{field_name} must be a finite non-negative number")
            try:
                normalized = float(value)
            except (OverflowError, TypeError, ValueError) as exc:
                raise RiskInputError(
                    f"{field_name} must be a finite non-negative number"
                ) from exc
            if not math.isfinite(normalized) or normalized < 0:
                raise RiskInputError(f"{field_name} must be a finite non-negative number")
            object.__setattr__(self, field_name, normalized)


class RiskLevel(str, Enum):
    """Ordinal severity bands for a window's risk score."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    """Immutable component scores and their derived overall classification."""

    precipitation_rate_score: int
    wind_speed_score: int
    wind_gust_score: int
    visibility_score: int

    @property
    def risk_score(self) -> int:
        return max(
            self.precipitation_rate_score,
            self.wind_speed_score,
            self.wind_gust_score,
            self.visibility_score,
        )

    @property
    def risk_level(self) -> RiskLevel:
        return classify_score(self.risk_score)

    @property
    def rules_version(self) -> str:
        return RULES_VERSION

    def components_dict(self) -> dict[str, int]:
        """Return a new mapping so callers cannot mutate this result."""
        return {
            "precipitation_rate": self.precipitation_rate_score,
            "wind_speed": self.wind_speed_score,
            "wind_gust": self.wind_gust_score,
            "visibility": self.visibility_score,
        }


def _greater_equal_score(value: float, thresholds: tuple[tuple[float, int], ...]) -> int:
    return max((score for threshold, score in thresholds if value >= threshold), default=0)


def _visibility_score(value: float) -> int:
    for threshold, score in _VISIBILITY_THRESHOLDS:
        if value < threshold:
            return score
    return 0


def calculate_risk(inputs: RiskInputs) -> RiskAssessment:
    """Return component scores from the fixed ``mvp-1`` thresholds."""
    if not isinstance(inputs, RiskInputs):
        raise RiskInputError("inputs must be a RiskInputs instance")
    return RiskAssessment(
        precipitation_rate_score=_greater_equal_score(
            inputs.precipitation_rate_max_mm_per_hour,
            _PRECIPITATION_THRESHOLDS,
        ),
        wind_speed_score=_greater_equal_score(
            inputs.wind_speed_max,
            _WIND_SPEED_THRESHOLDS,
        ),
        wind_gust_score=_greater_equal_score(inputs.wind_gust_max, _WIND_GUST_THRESHOLDS),
        visibility_score=_visibility_score(inputs.visibility_min),
    )


def classify_score(score: int) -> RiskLevel:
    """Classify any integer score from 0 through 100 into an ordinal band."""
    if type(score) is not int or not 0 <= score <= 100:
        raise RiskInputError("risk score must be an integer from 0 through 100")
    if score < 25:
        return RiskLevel.LOW
    if score < 50:
        return RiskLevel.MEDIUM
    if score < 75:
        return RiskLevel.HIGH
    return RiskLevel.CRITICAL


def _validate_metric_contract(metric: Mapping[str, object]) -> None:
    if set(metric) != WINDOW_METRIC_FIELDS:
        raise RiskInputError("metric keys do not match the window_aggregate contract")
    if metric["component"] != "flink":
        raise RiskInputError("metric component must be 'flink'")
    if metric["operation"] != "window_aggregate":
        raise RiskInputError("metric operation must be 'window_aggregate'")
    if type(metric["schema_version"]) is not int or metric["schema_version"] != 1:
        raise RiskInputError("metric schema_version must be integer 1")

    location_id = metric["location_id"]
    if not isinstance(location_id, str) or not location_id.strip():
        raise RiskInputError("metric location_id must be a non-empty string")

    event_count = metric["event_count"]
    if type(event_count) is not int or event_count <= 0:
        raise RiskInputError("metric event_count must be a positive integer")

    start_ms = metric["window_start_ms"]
    end_ms = metric["window_end_ms"]
    if type(start_ms) is not int or type(end_ms) is not int or end_ms <= start_ms:
        raise RiskInputError("metric window boundaries must be ordered integers")

    first_ms = metric["first_event_timestamp_ms"]
    last_ms = metric["last_event_timestamp_ms"]
    if (
        type(first_ms) is not int
        or type(last_ms) is not int
        or not start_ms <= first_ms <= last_ms < end_ms
    ):
        raise RiskInputError("metric event timestamps must be ordered within the window")


def inputs_from_metric(metric: Mapping[str, object]) -> RiskInputs:
    """Validate and extract the four risk factors from a window metric."""
    if not isinstance(metric, Mapping):
        raise RiskInputError("metric must be a mapping")
    _validate_metric_contract(metric)
    return RiskInputs(
        precipitation_rate_max_mm_per_hour=metric[
            "precipitation_rate_max_mm_per_hour"
        ],
        wind_speed_max=metric["wind_speed_max"],
        wind_gust_max=metric["wind_gust_max"],
        visibility_min=metric["visibility_min"],
    )


def enrich_metric(metric: Mapping[str, object]) -> dict[str, object]:
    """Copy a validated window metric and append its deterministic assessment."""
    inputs = inputs_from_metric(metric)
    assessment = calculate_risk(inputs)
    enriched = dict(metric)
    enriched["operation"] = "risk_assessment"
    enriched["risk_score"] = assessment.risk_score
    enriched["risk_level"] = assessment.risk_level.value
    enriched["risk_components"] = assessment.components_dict()
    enriched["risk_rules_version"] = assessment.rules_version
    return enriched
