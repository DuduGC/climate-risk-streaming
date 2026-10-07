"""Normalização sem efeitos colaterais do bloco current da Open-Meteo."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from math import isfinite

from climate_logistics.models import Location, WeatherEvent, WeatherValidationError

_EXPECTED_UNITS = {
    "time": "unixtime",
    "interval": "seconds",
    "temperature_2m": "°C",
    "precipitation": "mm",
    "wind_speed_10m": "km/h",
    "wind_gusts_10m": "km/h",
    "visibility": "m",
    "weather_code": "wmo code",
}


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise WeatherValidationError(f"{name} deve ser um objeto")
    return value


def _required(mapping: Mapping[str, object], key: str, section: str) -> object:
    try:
        return mapping[key]
    except KeyError as exc:
        raise WeatherValidationError(f"campo ausente: {section}.{key}") from exc


def _event_datetime(value: object) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WeatherValidationError("current.time deve ser timestamp Unix numérico")
    if isinstance(value, float) and not isfinite(value):
        raise WeatherValidationError("current.time deve ser timestamp Unix finito")
    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise WeatherValidationError("current.time está fora do domínio datetime") from exc


def _check_units(units: Mapping[str, object]) -> None:
    for field, expected in _EXPECTED_UNITS.items():
        actual = _required(units, field, "current_units")
        if actual != expected:
            raise WeatherValidationError(
                f"unidade incompatível em current_units.{field}: esperado {expected}"
            )


def normalize_current(
    payload: Mapping[str, object],
    location: Location,
    *,
    ingestion_timestamp: datetime,
) -> WeatherEvent:
    """Converte um payload current em evento validado, sem fazer chamadas HTTP."""
    data = _mapping(payload, "payload")
    if not isinstance(location, Location):
        raise WeatherValidationError("location deve ser Location")
    current = _mapping(_required(data, "current", "payload"), "current")
    units = _mapping(_required(data, "current_units", "payload"), "current_units")
    _check_units(units)

    event_timestamp = _event_datetime(_required(current, "time", "current"))
    interval = _required(current, "interval", "current")
    values = {
        "temperature": _required(current, "temperature_2m", "current"),
        "precipitation": _required(current, "precipitation", "current"),
        "wind_speed": _required(current, "wind_speed_10m", "current"),
        "wind_gust": _required(current, "wind_gusts_10m", "current"),
        "visibility": _required(current, "visibility", "current"),
        "weather_code": _required(current, "weather_code", "current"),
    }
    return WeatherEvent(
        location_id=location.location_id,
        city=location.city,
        latitude=location.latitude,
        longitude=location.longitude,
        temperature=values["temperature"],
        precipitation=values["precipitation"],
        wind_speed=values["wind_speed"],
        wind_gust=values["wind_gust"],
        visibility=values["visibility"],
        weather_code=values["weather_code"],
        measurement_interval_seconds=interval,
        event_timestamp=event_timestamp,
        ingestion_timestamp=ingestion_timestamp,
    )
