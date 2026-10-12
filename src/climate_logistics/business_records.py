"""Pure validation and deterministic identities for persisted risk assessments."""

from __future__ import annotations

import hashlib
import json
import math
from collections import OrderedDict
from collections.abc import Mapping

from climate_logistics.risk_engine import RULES_VERSION, calculate_risk, inputs_from_metric
from climate_logistics.temporal_metrics import WINDOW_METRIC_FIELDS

AssessmentIdentity = tuple[str, int, int, str]

ASSESSMENT_FIELDS = WINDOW_METRIC_FIELDS | frozenset(
    {"risk_score", "risk_level", "risk_components", "risk_rules_version"}
)
ALERT_FIELDS = ASSESSMENT_FIELDS | frozenset({"alert_id"})

_MAX_JSON_BYTES = 16 * 1024
_COMPONENT_FIELDS = frozenset(
    {"precipitation_rate", "wind_speed", "wind_gust", "visibility"}
)
_INTEGER_FIELDS = frozenset(
    {
        "schema_version",
        "window_start_ms",
        "window_end_ms",
        "event_count",
        "measurement_interval_min_seconds",
        "measurement_interval_max_seconds",
        "first_event_timestamp_ms",
        "last_event_timestamp_ms",
        "risk_score",
    }
)
_NUMBER_FIELDS = frozenset(
    {
        "temperature_avg",
        "precipitation_max",
        "precipitation_rate_max_mm_per_hour",
        "wind_speed_max",
        "wind_gust_max",
        "visibility_min",
    }
)
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


class BusinessRecordError(ValueError):
    """Raised when a business assessment does not satisfy its contract."""


def _json_constant(value: str) -> None:
    raise BusinessRecordError("assessment JSON contains a non-standard numeric constant")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise BusinessRecordError("assessment JSON contains a duplicate object key")
        result[key] = value
    return result


def _require_integer(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise BusinessRecordError(f"{field_name} must be an integer")
    return value


def _require_utf8_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessRecordError(f"{field_name} must be non-empty text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise BusinessRecordError(f"{field_name} must be valid UTF-8 text") from exc
    return value


def _require_finite_number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise BusinessRecordError(f"{field_name} must be a finite number")
    try:
        finite = math.isfinite(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise BusinessRecordError(f"{field_name} must be a finite number") from exc
    if not finite:
        raise BusinessRecordError(f"{field_name} must be a finite number")
    return float(value)


def _validate_identity(identity: object) -> AssessmentIdentity:
    if type(identity) is not tuple or len(identity) != 4:
        raise BusinessRecordError("assessment identity must be a four-item tuple")
    location_id, window_start_ms, window_end_ms, rules_version = identity
    _require_utf8_text(location_id, "assessment identity location_id")
    _require_utf8_text(rules_version, "assessment identity rules version")
    start = _require_integer(window_start_ms, "window_start_ms")
    end = _require_integer(window_end_ms, "window_end_ms")
    if not _INT64_MIN <= start <= _INT64_MAX or not _INT64_MIN <= end <= _INT64_MAX:
        raise BusinessRecordError("assessment identity timestamps must fit PostgreSQL BIGINT")
    if end <= start:
        raise BusinessRecordError("assessment identity window boundaries must be ordered")
    return location_id, start, end, rules_version


def _validate_assessment(assessment: object) -> dict[str, object]:
    if not isinstance(assessment, Mapping):
        raise BusinessRecordError("assessment must be an object")

    try:
        fields = set(assessment)
    except (TypeError, ValueError) as exc:
        raise BusinessRecordError("assessment keys are invalid") from exc
    if fields != ASSESSMENT_FIELDS:
        raise BusinessRecordError("assessment keys do not match the 21-field contract")

    normalized = dict(assessment)
    if type(normalized["component"]) is not str or normalized["component"] != "flink":
        raise BusinessRecordError("assessment component must be 'flink'")
    if (
        type(normalized["operation"]) is not str
        or normalized["operation"] != "risk_assessment"
    ):
        raise BusinessRecordError("assessment operation must be 'risk_assessment'")
    if _require_integer(normalized["schema_version"], "schema_version") != 1:
        raise BusinessRecordError("assessment schema_version must be integer 1")
    if (
        type(normalized["risk_rules_version"]) is not str
        or normalized["risk_rules_version"] != RULES_VERSION
    ):
        raise BusinessRecordError("assessment risk rules version is unsupported")

    normalized["location_id"] = _require_utf8_text(
        normalized["location_id"], "assessment location_id"
    )

    for field_name in _INTEGER_FIELDS - {"schema_version", "risk_score"}:
        value = _require_integer(normalized[field_name], field_name)
        if field_name.endswith("_ms") and not _INT64_MIN <= value <= _INT64_MAX:
            raise BusinessRecordError(f"{field_name} must fit PostgreSQL BIGINT")

    event_count = _require_integer(normalized["event_count"], "event_count")
    if not 0 < event_count <= _INT64_MAX:
        raise BusinessRecordError("event_count must be a positive PostgreSQL BIGINT")
    start_ms = normalized["window_start_ms"]
    end_ms = normalized["window_end_ms"]
    if end_ms <= start_ms:
        raise BusinessRecordError("assessment window boundaries must be ordered")
    first_ms = normalized["first_event_timestamp_ms"]
    last_ms = normalized["last_event_timestamp_ms"]
    if not start_ms <= first_ms <= last_ms < end_ms:
        raise BusinessRecordError("assessment event timestamps must be ordered in the window")

    min_interval = normalized["measurement_interval_min_seconds"]
    max_interval = normalized["measurement_interval_max_seconds"]
    if not 1 <= min_interval <= max_interval <= 3600:
        raise BusinessRecordError("measurement intervals must be ordered between 1 and 3600")

    for field_name in _NUMBER_FIELDS:
        _require_finite_number(normalized[field_name], field_name)
    if normalized["precipitation_max"] < 0:
        raise BusinessRecordError("precipitation_max must be non-negative")

    components = normalized["risk_components"]
    if not isinstance(components, Mapping) or set(components) != _COMPONENT_FIELDS:
        raise BusinessRecordError("risk_components must contain exactly four named scores")
    normalized_components: dict[str, int] = {}
    for field_name, value in components.items():
        normalized_components[field_name] = _require_integer(value, field_name)
    normalized["risk_components"] = normalized_components

    risk_score = _require_integer(normalized["risk_score"], "risk_score")
    risk_level = normalized["risk_level"]
    if type(risk_level) is not str:
        raise BusinessRecordError("risk_level must be text")

    metric = {field: normalized[field] for field in WINDOW_METRIC_FIELDS}
    metric["operation"] = "window_aggregate"
    try:
        risk = calculate_risk(inputs_from_metric(metric))
    except (TypeError, ValueError, OverflowError) as exc:
        raise BusinessRecordError("assessment risk inputs are invalid") from exc

    if risk_score != risk.risk_score:
        raise BusinessRecordError("risk_score does not match the assessed factors")
    if risk_level != risk.risk_level.value:
        raise BusinessRecordError("risk_level does not match risk_score")
    if normalized_components != risk.components_dict():
        raise BusinessRecordError("risk_components do not match the assessed factors")

    return normalized


def parse_assessment(raw: str) -> dict[str, object]:
    """Parse and validate one compact 21-field risk assessment JSON value."""
    if not isinstance(raw, str):
        raise BusinessRecordError("assessment JSON must be text")
    try:
        encoded = raw.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise BusinessRecordError("assessment JSON must be valid UTF-8") from exc
    if len(encoded) > _MAX_JSON_BYTES:
        raise BusinessRecordError("assessment JSON exceeds 16 KiB")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_json_constant,
        )
    except BusinessRecordError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
        raise BusinessRecordError("assessment JSON is invalid") from exc
    return _validate_assessment(value)


def assessment_identity(assessment: Mapping[str, object]) -> AssessmentIdentity:
    """Validate an assessment and return its stable logical identity."""
    normalized = _validate_assessment(assessment)
    identity = (
        normalized["location_id"],
        normalized["window_start_ms"],
        normalized["window_end_ms"],
        normalized["risk_rules_version"],
    )
    return _validate_identity(identity)


def alert_id(identity: AssessmentIdentity) -> str:
    """Hash the canonical compact UTF-8 JSON list for one assessment identity."""
    normalized = _validate_identity(identity)
    try:
        canonical = json.dumps(
            list(normalized),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise BusinessRecordError("assessment identity cannot be encoded as UTF-8 JSON") from exc
    return hashlib.sha256(canonical).hexdigest()


def make_alert(assessment: Mapping[str, object]) -> dict[str, object] | None:
    """Return the 22-field alert for a valid HIGH/CRITICAL assessment."""
    normalized = _validate_assessment(assessment)
    if normalized["risk_score"] < 50:
        return None

    alert = dict(normalized)
    alert["operation"] = "weather_alert"
    alert["alert_id"] = alert_id(
        (
            normalized["location_id"],
            normalized["window_start_ms"],
            normalized["window_end_ms"],
            normalized["risk_rules_version"],
        )
    )
    return alert


def _validate_alert(alert: object) -> dict[str, object]:
    if not isinstance(alert, Mapping):
        raise BusinessRecordError("alert must be an object")
    try:
        fields = set(alert)
    except (TypeError, ValueError) as exc:
        raise BusinessRecordError("alert keys are invalid") from exc
    if fields != ALERT_FIELDS:
        raise BusinessRecordError("alert keys do not match the 22-field contract")

    candidate = dict(alert)
    if type(candidate["operation"]) is not str or candidate["operation"] != "weather_alert":
        raise BusinessRecordError("alert operation must be 'weather_alert'")
    alert_id_value = _require_utf8_text(candidate["alert_id"], "alert_id")
    assessment = dict(candidate)
    del assessment["alert_id"]
    assessment["operation"] = "risk_assessment"
    normalized_assessment = _validate_assessment(assessment)
    expected = make_alert(normalized_assessment)
    if expected is None:
        raise BusinessRecordError("alert risk must be HIGH or CRITICAL")
    if alert_id_value != expected["alert_id"]:
        raise BusinessRecordError("alert_id does not match the assessment identity")
    return expected


def parse_alert(raw: str) -> dict[str, object]:
    """Parse and validate one compact 22-field HIGH/CRITICAL alert JSON value."""
    if not isinstance(raw, str):
        raise BusinessRecordError("alert JSON must be text")
    try:
        encoded = raw.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise BusinessRecordError("alert JSON must be valid UTF-8") from exc
    if len(encoded) > _MAX_JSON_BYTES:
        raise BusinessRecordError("alert JSON exceeds 16 KiB")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_json_constant,
        )
    except BusinessRecordError:
        raise
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        TypeError,
        ValueError,
        RecursionError,
    ) as exc:
        raise BusinessRecordError("alert JSON is invalid") from exc
    return _validate_alert(value)


def alert_fingerprint(alert: Mapping[str, object]) -> str:
    """Return a stable SHA-256 fingerprint for one validated alert payload."""
    normalized = _validate_alert(alert)
    for field_name in _NUMBER_FIELDS:
        normalized[field_name] = float(normalized[field_name])
    try:
        canonical = json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise BusinessRecordError("alert cannot be serialized as JSON") from exc
    return hashlib.sha256(canonical).hexdigest()


class AlertIdCache:
    """Bounded in-memory FIFO cache of alert IDs and normalized fingerprints."""

    def __init__(self, capacity: int = 10_000) -> None:
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._fingerprints: OrderedDict[str, str] = OrderedDict()

    def classify(self, alert: Mapping[str, object]) -> str:
        """Classify an alert without changing cache insertion order or contents."""
        normalized = _validate_alert(alert)
        identity = normalized["alert_id"]
        fingerprint = alert_fingerprint(normalized)
        previous = self._fingerprints.get(identity)
        if previous is None:
            return "new"
        return "duplicate" if previous == fingerprint else "divergent"

    def remember(self, alert: Mapping[str, object]) -> None:
        """Store a confirmed new alert, evicting the oldest insertion if full."""
        normalized = _validate_alert(alert)
        identity = normalized["alert_id"]
        if identity in self._fingerprints:
            return
        self._fingerprints[identity] = alert_fingerprint(normalized)
        if len(self._fingerprints) > self._capacity:
            self._fingerprints.popitem(last=False)


def jdbc_values(assessment: Mapping[str, object]) -> tuple[str, str | None]:
    """Return deterministic assessment JSON and its alert ID when eligible."""
    normalized = _validate_assessment(assessment)
    try:
        payload = json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise BusinessRecordError("assessment cannot be serialized as JSON") from exc

    eligible_id: str | None = None
    if normalized["risk_score"] >= 50:
        eligible_id = alert_id(
            (
                normalized["location_id"],
                normalized["window_start_ms"],
                normalized["window_end_ms"],
                normalized["risk_rules_version"],
            )
        )
    return payload, eligible_id
