"""Normalização sem efeitos colaterais do bloco current da Open-Meteo."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from math import isfinite

import requests

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

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_MAX_RESPONSE_BYTES = 131_072
_RESPONSE_CHUNK_SIZE = 8192
_MAX_ATTEMPTS = 3
_BACKOFF_SECONDS = (0.5, 1.0)
_RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})


class OpenMeteoError(RuntimeError):
    """Falha de transporte ou resposta da API Open-Meteo."""


def utc_now() -> datetime:
    """Retorna o horário atual com fuso UTC explícito."""
    return datetime.now(UTC)


def _backoff_seconds(attempt: int) -> float:
    return _BACKOFF_SECONDS[attempt]


def _retry_after_seconds(response: requests.Response, fallback: float) -> float:
    raw_value = response.headers.get("Retry-After")
    if not isinstance(raw_value, str):
        return fallback
    try:
        delay = float(raw_value.strip())
    except ValueError:
        return fallback
    if not isfinite(delay) or delay < 0:
        return fallback
    return min(delay, 5.0)


def _read_limited_body(response: requests.Response, location_id: str) -> bytes:
    content = bytearray()
    for chunk in response.iter_content(chunk_size=_RESPONSE_CHUNK_SIZE):
        if not chunk:
            continue
        if not isinstance(chunk, bytes):
            raise OpenMeteoError(
                f"resposta inválida da Open-Meteo para {location_id}"
            )
        if len(content) + len(chunk) > _MAX_RESPONSE_BYTES:
            raise OpenMeteoError(
                f"resposta da Open-Meteo excede o limite para {location_id}"
            )
        content.extend(chunk)
    return bytes(content)


class OpenMeteoClient:
    """Busca e normaliza uma resposta current da API Open-Meteo."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._owns_session = session is None
        self._session = requests.Session() if session is None else session
        if self._owns_session:
            self._session.trust_env = False
        self._sleeper = sleeper
        self._clock = clock
        self._closed = False

    def fetch(self, location: Location) -> WeatherEvent:
        if self._closed:
            raise OpenMeteoError("cliente Open-Meteo já foi fechado")
        if not isinstance(location, Location):
            raise WeatherValidationError("location deve ser Location")

        params: dict[str, object] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "current": (
                "temperature_2m,precipitation,wind_speed_10m,"
                "wind_gusts_10m,visibility,weather_code"
            ),
            "temperature_unit": "celsius",
            "precipitation_unit": "mm",
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
            "timeformat": "unixtime",
        }

        for attempt in range(_MAX_ATTEMPTS):
            response: requests.Response | None = None
            retry_delay: float | None = None
            try:
                response = self._session.get(
                    OPEN_METEO_URL,
                    params=params,
                    timeout=(3, 10),
                    stream=True,
                    allow_redirects=False,
                    verify=True,
                )
                status = response.status_code
                if status == 429 or status in _RETRYABLE_STATUSES:
                    if attempt == _MAX_ATTEMPTS - 1:
                        raise OpenMeteoError(
                            f"Open-Meteo retornou HTTP {status} para {location.location_id} "
                            f"após {_MAX_ATTEMPTS} tentativas"
                        )
                    fallback = _backoff_seconds(attempt)
                    retry_delay = (
                        _retry_after_seconds(response, fallback)
                        if status == 429
                        else fallback
                    )
                elif status != 200:
                    raise OpenMeteoError(
                        f"Open-Meteo retornou HTTP {status} para {location.location_id}"
                    )
                else:
                    body = _read_limited_body(response, location.location_id)
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except (ValueError, RecursionError):
                        raise OpenMeteoError(
                            f"resposta JSON inválida da Open-Meteo para "
                            f"{location.location_id}"
                        ) from None
                    ingestion_timestamp = self._clock()
                    try:
                        return normalize_current(
                            payload,
                            location,
                            ingestion_timestamp=ingestion_timestamp,
                        )
                    except WeatherValidationError as exc:
                        raise WeatherValidationError(
                            f"contrato climático inválido para {location.location_id}: {exc}"
                        ) from exc
            except requests.exceptions.SSLError as exc:
                raise OpenMeteoError(
                    f"falha TLS ao consultar Open-Meteo para {location.location_id}"
                ) from exc
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                if attempt == _MAX_ATTEMPTS - 1:
                    raise OpenMeteoError(
                        f"falha de transporte Open-Meteo para {location.location_id} "
                        f"após {_MAX_ATTEMPTS} tentativas"
                    ) from exc
                retry_delay = _backoff_seconds(attempt)
            except WeatherValidationError:
                raise
            except OpenMeteoError:
                raise
            except requests.exceptions.RequestException as exc:
                raise OpenMeteoError(
                    f"falha HTTP ao consultar Open-Meteo para {location.location_id}"
                ) from exc
            finally:
                if response is not None:
                    response.close()

            if retry_delay is not None:
                self._sleeper(retry_delay)

        raise OpenMeteoError(f"falha de transporte Open-Meteo para {location.location_id}")

    def close(self) -> None:
        """Fecha a sessão apenas quando ela pertence ao cliente."""
        if self._closed:
            return
        self._closed = True
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> OpenMeteoClient:
        if self._closed:
            raise OpenMeteoError("cliente Open-Meteo já foi fechado")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()