"""Política temporal pura e identidade lógica dos eventos meteorológicos."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from climate_logistics.models import WeatherEvent

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FUTURE_TOLERANCE = timedelta(minutes=5)
MAX_INGESTION_DELAY = timedelta(hours=24)
DEFAULT_DEDUP_TTL_SECONDS = 86_400

DataQualityReason = Literal[
    "timestamp_before_epoch",
    "event_after_ingestion",
    "ingestion_in_future",
    "excessive_ingestion_delay",
]


class DataQualityError(ValueError):
    """Evento válido no schema, mas temporalmente inconsistente."""

    def __init__(self, reason: DataQualityReason) -> None:
        self.reason = reason
        super().__init__(reason)


def _observed_clock_utc(observed_at: datetime) -> datetime:
    """Converte a configuração do relógio, sem classificá-la como evento ruim."""
    if not isinstance(observed_at, datetime):
        raise ValueError("observed_at deve ser um datetime com fuso horário")

    try:
        offset = observed_at.utcoffset()
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("observed_at não pode ser convertido para UTC") from exc
    if observed_at.tzinfo is None or offset is None:
        raise ValueError("observed_at deve ser um datetime com fuso horário")

    try:
        return observed_at.astimezone(UTC)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("observed_at não pode ser convertido para UTC") from exc


def validate_event_time(event: WeatherEvent, *, observed_at: datetime) -> None:
    """Aplica limites temporais inclusivos usando o relógio UTC injetado.

    A ordem de precedência das rejeições é: timestamp antes da época, evento
    mais de cinco minutos após a ingestão, ingestão mais de cinco minutos à
    frente do relógio observado e atraso de ingestão superior a 24 horas.
    """
    observed_utc = _observed_clock_utc(observed_at)
    event_utc = event.event_timestamp.astimezone(UTC)
    ingestion_utc = event.ingestion_timestamp.astimezone(UTC)

    if event_utc < EPOCH or ingestion_utc < EPOCH:
        raise DataQualityError("timestamp_before_epoch")
    if event_utc - ingestion_utc > FUTURE_TOLERANCE:
        raise DataQualityError("event_after_ingestion")
    if ingestion_utc - observed_utc > FUTURE_TOLERANCE:
        raise DataQualityError("ingestion_in_future")
    if ingestion_utc - event_utc > MAX_INGESTION_DELAY:
        raise DataQualityError("excessive_ingestion_delay")


def event_identity(event: WeatherEvent) -> tuple[str, str]:
    """Retorna localidade e timestamp canônico UTC/Z, sem dados de ingestão."""
    serialized = event.to_dict()
    return event.location_id, str(serialized["event_timestamp"])
