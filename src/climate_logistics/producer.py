"""CLI para coletar eventos da Open-Meteo e publicar em Kafka."""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import sys
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from climate_logistics.kafka_publisher import (
    KafkaPublisher,
    KafkaPublishError,
    KafkaSerializationError,
    KafkaSetupError,
    emit_json_log,
    ensure_topics,
)
from climate_logistics.locations import LOCATIONS, validate_locations
from climate_logistics.models import WeatherEvent, WeatherValidationError
from climate_logistics.open_meteo import OpenMeteoClient

_LOGGER = logging.getLogger("climate_logistics.producer")


class ProducerConfigurationError(ValueError):
    """Configuração de execução inválida."""


@dataclass(frozen=True)
class ProducerSettings:
    bootstrap_servers: str
    poll_interval_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ProducerSettings:
        bootstrap = env.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        if (
            not isinstance(bootstrap, str)
            or not bootstrap
            or bootstrap != bootstrap.strip()
            or any(character.isspace() for character in bootstrap)
            or "://" in bootstrap
            or "@" in bootstrap
            or any(character in bootstrap for character in "/?#")
            or any(not broker for broker in bootstrap.split(","))
        ):
            raise ProducerConfigurationError("KAFKA_BOOTSTRAP_SERVERS inválido")

        raw_interval = env.get("WEATHER_POLL_INTERVAL_SECONDS", "300")
        if not isinstance(raw_interval, str) or re.fullmatch(r"[0-9]+", raw_interval) is None:
            raise ProducerConfigurationError("WEATHER_POLL_INTERVAL_SECONDS deve ser inteiro")
        interval = int(raw_interval)
        if not 1 <= interval <= 86_400:
            raise ProducerConfigurationError(
                "WEATHER_POLL_INTERVAL_SECONDS deve estar entre 1 e 86400"
            )
        return cls(bootstrap, interval)


def run_cycle(
    client: OpenMeteoClient,
    publisher: KafkaPublisher,
    locations: Sequence[Any] = LOCATIONS,
    *,
    stop: threading.Event,
    logger: logging.Logger,
) -> bool:
    """Busca cada localidade, publica eventos válidos e confirma o lote."""
    validate_locations(locations)
    emit_json_log(logger, "cycle", "started", component="producer")
    collection_failures = 0
    collected = 0
    accepted = 0
    stopped_early = False
    fatal: KafkaPublishError | None = None

    for index, location in enumerate(locations):
        if stop.is_set():
            stopped_early = True
            break
        try:
            event = client.fetch(location)
            if not isinstance(event, WeatherEvent):
                raise WeatherValidationError("fetch não retornou WeatherEvent")
            if event.location_id != location.location_id:
                raise WeatherValidationError("event.location_id não corresponde à localidade")
        except Exception as exc:
            collection_failures += 1
            emit_json_log(
                logger,
                "collection",
                "error",
                component="producer",
                location_id=location.location_id,
                error=type(exc).__name__,
            )
            continue

        collected += 1
        try:
            publisher.enqueue(event)
            accepted += 1
        except KafkaSerializationError as exc:
            collection_failures += 1
            emit_json_log(
                logger,
                "serialization",
                "error",
                component="producer",
                location_id=location.location_id,
                error=type(exc).__name__,
            )
        except KafkaPublishError as exc:
            fatal = exc
            break
        except Exception as exc:
            fatal = KafkaPublishError("falha inesperada ao enfileirar evento Kafka")
            fatal.__cause__ = exc
            break

    try:
        delivered = publisher.finish_batch()
    except KafkaPublishError as exc:
        if fatal is None:
            fatal = exc
        delivered = exc.delivered_count
    except Exception as exc:
        if fatal is None:
            fatal = KafkaPublishError("falha inesperada ao confirmar lote Kafka")
            fatal.__cause__ = exc
        delivered = 0
    if fatal is not None:
        emit_json_log(
            logger,
            "cycle",
            "failed",
            component="producer",
            collected=collected,
            accepted=accepted,
            delivered=delivered,
            failures=collection_failures,
            error=type(fatal).__name__,
        )
        raise fatal

    if stop.is_set() and index < len(locations) - 1:
        stopped_early = True
    complete = (
        not stopped_early
        and collection_failures == 0
        and collected == len(locations)
        and accepted == len(locations)
        and delivered == accepted
    )
    emit_json_log(
        logger,
        "cycle",
        "success" if complete else "partial",
        component="producer",
        collected=collected,
        accepted=accepted,
        delivered=delivered,
        failures=collection_failures,
    )
    return complete


def _install_signal_handlers(stop: threading.Event) -> dict[int, Any]:
    previous: dict[int, Any] = {}
    if threading.current_thread() is not threading.main_thread():
        return previous

    def request_stop(signum: int, frame: object) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, request_stop)
    return previous


def _restore_signal_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    existing = next(
        (handler for handler in root.handlers if getattr(handler, "_climate_producer_json", False)),
        None,
    )
    if existing is not None:
        existing.stream = sys.stdout
        existing.setLevel(logging.INFO)
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    setattr(handler, "_climate_producer_json", True)
    root.addHandler(handler)

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Coleta clima e publica eventos em weather.raw")
    parser.add_argument("--once", action="store_true", help="executa um ciclo e encerra")
    args = parser.parse_args(argv)

    _configure_logging()

    try:
        settings = ProducerSettings.from_env(os.environ)
    except ProducerConfigurationError as exc:
        emit_json_log(
            _LOGGER,
            "configuration",
            "error",
            component="producer",
            error=type(exc).__name__,
        )
        return 1

    stop = threading.Event()
    previous_handlers: dict[int, Any] = {}
    client: OpenMeteoClient | None = None
    publisher: KafkaPublisher | None = None
    exit_code = 0
    try:
        ensure_topics(settings.bootstrap_servers)
        publisher = KafkaPublisher(settings.bootstrap_servers, logger=_LOGGER)
        client = OpenMeteoClient()
        previous_handlers = _install_signal_handlers(stop)
        while not stop.is_set():
            cycle_complete = run_cycle(
                client,
                publisher,
                stop=stop,
                logger=_LOGGER,
            )
            if args.once:
                exit_code = 0 if cycle_complete else 1
                break
            if not stop.is_set():
                stop.wait(settings.poll_interval_seconds)
    except (KafkaSetupError, KafkaPublishError) as exc:
        emit_json_log(
            _LOGGER,
            "runtime",
            "error",
            component="producer",
            error=type(exc).__name__,
        )
        exit_code = 1
    except Exception as exc:
        emit_json_log(
            _LOGGER,
            "runtime",
            "error",
            component="producer",
            error=type(exc).__name__,
        )
        exit_code = 1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                emit_json_log(
                    _LOGGER,
                    "http_client",
                    "error",
                    component="producer",
                    error=type(exc).__name__,
                )
                exit_code = 1
        if publisher is not None:
            try:
                publisher.close()
            except KafkaPublishError as exc:
                emit_json_log(
                    _LOGGER,
                    "publisher",
                    "error",
                    component="producer",
                    error=type(exc).__name__,
                )
                exit_code = 1
            except Exception as exc:
                emit_json_log(
                    _LOGGER,
                    "publisher",
                    "error",
                    component="producer",
                    error=type(exc).__name__,
                )
                exit_code = 1
        try:
            _restore_signal_handlers(previous_handlers)
        except Exception as exc:
            emit_json_log(
                _LOGGER,
                "signals",
                "error",
                component="producer",
                error=type(exc).__name__,
            )
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
