"""Modelos imutáveis e validação do contrato de dados climáticos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite


class WeatherValidationError(ValueError):
    """Valor incompatível com o contrato de clima da plataforma."""


def _non_empty_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WeatherValidationError(f"{field_name} deve ser texto não vazio")
    return value


def _bounded_number(value: object, field_name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WeatherValidationError(f"{field_name} deve ser um número finito")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise WeatherValidationError(f"{field_name} deve ser um número finito") from exc
    if not isfinite(number) or not minimum <= number <= maximum:
        raise WeatherValidationError(
            f"{field_name} deve estar entre {minimum} e {maximum}"
        )
    return number


def _aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise WeatherValidationError(f"{field_name} deve ser datetime com fuso horário")
    try:
        value.astimezone(UTC)
    except OverflowError as exc:
        raise WeatherValidationError(
            f"{field_name} não pode ser representado em UTC"
        ) from exc
    return value

def _interval(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 3600:
        raise WeatherValidationError("measurement_interval_seconds deve ser inteiro entre 1 e 3600")
    return value


@dataclass(frozen=True)
class Location:
    """Localidade configurada para coleta de dados meteorológicos."""

    location_id: str
    city: str
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        _non_empty_text(self.location_id, "location_id")
        _non_empty_text(self.city, "city")
        object.__setattr__(
            self, "latitude", _bounded_number(self.latitude, "latitude", -90, 90)
        )
        object.__setattr__(
            self, "longitude", _bounded_number(self.longitude, "longitude", -180, 180)
        )


@dataclass(frozen=True)
class WeatherEvent:
    """Evento normalizado com valores e timestamps validados."""

    location_id: str
    city: str
    latitude: float
    longitude: float
    temperature: float
    precipitation: float
    wind_speed: float
    wind_gust: float
    visibility: float
    weather_code: int
    measurement_interval_seconds: int
    event_timestamp: datetime
    ingestion_timestamp: datetime
    source: str = "open-meteo"
    schema_version: int = 1

    def __post_init__(self) -> None:
        _non_empty_text(self.location_id, "location_id")
        _non_empty_text(self.city, "city")
        for field_name, minimum, maximum in (
            ("latitude", -90, 90),
            ("longitude", -180, 180),
            ("temperature", -100, 100),
            ("precipitation", 0, 1000),
            ("wind_speed", 0, 500),
            ("wind_gust", 0, 500),
            ("visibility", 0, 1_000_000),
        ):
            object.__setattr__(
                self,
                field_name,
                _bounded_number(getattr(self, field_name), field_name, minimum, maximum),
            )

        if (
            isinstance(self.weather_code, bool)
            or not isinstance(self.weather_code, int)
            or self.weather_code not in _WMO_WEATHER_CODES
        ):
            raise WeatherValidationError("weather_code não pertence ao conjunto WMO aceito")
        _interval(self.measurement_interval_seconds)
        _aware_datetime(self.event_timestamp, "event_timestamp")
        _aware_datetime(self.ingestion_timestamp, "ingestion_timestamp")
        if self.source != "open-meteo":
            raise WeatherValidationError("source deve ser 'open-meteo'")
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != 1
        ):
            raise WeatherValidationError("schema_version deve ser 1")

    def to_dict(self) -> dict[str, object]:
        """Retorna valores escalares JSON e timestamps em UTC terminado em Z."""
        return {
            "location_id": self.location_id,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "temperature": self.temperature,
            "precipitation": self.precipitation,
            "wind_speed": self.wind_speed,
            "wind_gust": self.wind_gust,
            "visibility": self.visibility,
            "weather_code": self.weather_code,
            "measurement_interval_seconds": self.measurement_interval_seconds,
            "event_timestamp": self.event_timestamp.astimezone(UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "ingestion_timestamp": self.ingestion_timestamp.astimezone(UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "source": self.source,
            "schema_version": self.schema_version,
        }


_WMO_WEATHER_CODES = frozenset(
    {0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
     71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99}
)
