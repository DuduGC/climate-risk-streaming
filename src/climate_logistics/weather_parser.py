"""Parsing puro e contrato explícito da linha de diagnóstico Flink."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime

from climate_logistics.models import WeatherEvent, WeatherValidationError

MAX_MESSAGE_BYTES = 16_384

WEATHER_FIELDS = (
    "location_id",
    "city",
    "latitude",
    "longitude",
    "temperature",
    "precipitation",
    "wind_speed",
    "wind_gust",
    "visibility",
    "weather_code",
    "measurement_interval_seconds",
    "event_timestamp",
    "ingestion_timestamp",
    "source",
    "schema_version",
)

WEATHER_KINDS = (
    "STRING",
    "STRING",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "INT",
    "INT",
    "STRING",
    "STRING",
    "STRING",
    "INT",
)

_FIELD_INDEX = {field: index for index, field in enumerate(WEATHER_FIELDS)}


class WeatherParseError(ValueError):
    """Mensagem Kafka não corresponde ao contrato de evento climático v1."""


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise WeatherParseError(f"{field} deve ser timestamp ISO 8601 com fuso")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise WeatherParseError(f"{field} deve incluir fuso horário")
        return parsed.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        if isinstance(exc, WeatherParseError):
            raise
        raise WeatherParseError(f"{field} não é timestamp ISO 8601 válido") from exc


def parse_weather_event(raw: str) -> WeatherEvent:
    """Valida uma mensagem UTF-8/schema v1 sem registrar ou repetir o corpo."""
    if not isinstance(raw, str):
        raise WeatherParseError("mensagem deve ser texto UTF-8")
    try:
        message_size = len(raw.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise WeatherParseError("mensagem contém texto não representável em UTF-8") from exc
    if message_size > MAX_MESSAGE_BYTES:
        raise WeatherParseError(f"mensagem excede o limite de {MAX_MESSAGE_BYTES} bytes")

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise WeatherParseError("JSON inválido ou fora dos limites de parsing") from exc

    if not isinstance(payload, dict):
        raise WeatherParseError("evento deve ser um objeto JSON")
    if set(payload) != set(WEATHER_FIELDS):
        raise WeatherParseError("objeto JSON não corresponde aos campos do schema v1")

    for field, value in payload.items():
        if isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise WeatherParseError(
                    f"{field} contains text that cannot be encoded as UTF-8"
                ) from exc
    event_fields = dict(payload)
    event_fields["event_timestamp"] = _parse_timestamp(
        event_fields["event_timestamp"], "event_timestamp"
    )
    event_fields["ingestion_timestamp"] = _parse_timestamp(
        event_fields["ingestion_timestamp"], "ingestion_timestamp"
    )
    try:
        return WeatherEvent(**event_fields)
    except WeatherValidationError as exc:
        raise WeatherParseError(str(exc)) from exc


def event_values(event: WeatherEvent) -> tuple[object, ...]:
    """Extrai todos os campos na ordem estável do Row do Flink."""
    serialized = event.to_dict()
    return tuple(serialized[field] for field in WEATHER_FIELDS)


def diagnostic_summary(values: Sequence[object]) -> str:
    """Serializa somente a evidência mínima de que o parser processou o evento."""
    if len(values) != len(WEATHER_FIELDS):
        raise ValueError("valores não correspondem à linha WEATHER_FIELDS")
    summary = {
        "component": "flink",
        "operation": "parse",
        "status": "success",
        "location_id": values[_FIELD_INDEX["location_id"]],
        "event_timestamp": values[_FIELD_INDEX["event_timestamp"]],
        "ingestion_timestamp": values[_FIELD_INDEX["ingestion_timestamp"]],
        "schema_version": values[_FIELD_INDEX["schema_version"]],
    }
    return json.dumps(summary, ensure_ascii=False, separators=(",", ":"), allow_nan=False)