"""Serialização, preparação de tópicos e entrega confirmada em Kafka."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from climate_logistics.models import WeatherEvent

try:
    from confluent_kafka import KafkaError, KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:  # permite executar a suíte offline sem carregar a biblioteca opcional
    KafkaError = None  # type: ignore[assignment,misc]
    KafkaException = Exception  # type: ignore[assignment,misc]
    Producer = None  # type: ignore[assignment,misc]
    AdminClient = None  # type: ignore[assignment,misc]
    NewTopic = None  # type: ignore[assignment,misc]

TOPIC_ALREADY_EXISTS = (
    getattr(KafkaError, "TOPIC_ALREADY_EXISTS", 36) if KafkaError is not None else 36
)
RAW_TOPIC = "weather.raw"
ALERTS_TOPIC = "weather.alerts"
TOPICS = (RAW_TOPIC, ALERTS_TOPIC)
_REQUIRED_PARTITIONS = 3
_MAX_VALUE_BYTES = 16_384
_QUEUE_DEADLINE_SECONDS = 5.0
_FLUSH_TIMEOUT_SECONDS = 12.0
_TOPIC_DEADLINE_SECONDS = 15.0
_PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "gzip",
    "linger.ms": 20,
    "batch.size": 32_768,
    "delivery.timeout.ms": 10_000,
}


class KafkaSetupError(RuntimeError):
    """Falha ao consultar, criar ou conferir os tópicos obrigatórios."""


class KafkaPublishError(RuntimeError):
    """Falha ao publicar ou confirmar eventos."""

    def __init__(self, message: str, *, delivered_count: int = 0) -> None:
        super().__init__(message)
        self.delivered_count = delivered_count

class KafkaSerializationError(KafkaPublishError):
    """Evento incompatível com o contrato serializado para Kafka."""


def emit_json_log(
    logger: logging.Logger,
    operation: str,
    status: str,
    *,
    component: str = "kafka_publisher",
    **fields: object,
) -> None:
    """Escreve uma linha JSON pequena, sem configuração ou corpo da mensagem."""
    record: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "component": component,
        "operation": operation,
        "status": status,
    }
    record.update(fields)
    level = logging.ERROR if status in {"error", "failed"} else logging.INFO
    logger.log(level, json.dumps(record, ensure_ascii=False, separators=(",", ":")))


def serialize_event(event: WeatherEvent) -> tuple[bytes, bytes, int]:
    """Retorna chave UTF-8, JSON compacto UTF-8 e CreateTime em milissegundos."""
    try:
        value = json.dumps(
            event.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        key = event.location_id.encode("utf-8")
        timestamp_ms = int(event.ingestion_timestamp.timestamp() * 1000)
    except (AttributeError, TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise KafkaSerializationError("evento não pode ser serializado para Kafka") from exc
    if len(value) > _MAX_VALUE_BYTES:
        raise KafkaSerializationError(
            f"valor Kafka excede o limite de {_MAX_VALUE_BYTES} bytes"
        )
    return key, value, timestamp_ms


def _remaining(deadline: float, clock: Any, operation: str) -> float:
    remaining = deadline - clock()
    if remaining <= 0:
        raise KafkaSetupError(f"prazo total de {operation} excedido")
    return remaining


def _topic_already_exists(exc: BaseException) -> bool:
    for candidate in (exc, *exc.args):
        code = getattr(candidate, "code", None)
        if callable(code):
            try:
                if code() == TOPIC_ALREADY_EXISTS:
                    return True
            except Exception:
                continue
    return False


def _verify_topic(name: str, topic: object | None) -> None:
    if topic is None:
        raise KafkaSetupError(f"metadata não confirmou o tópico {name}")
    topic_error = getattr(topic, "error", None)
    if topic_error:
        raise KafkaSetupError(f"não foi possível consultar o tópico {name}")
    partitions = getattr(topic, "partitions", {})
    if len(partitions) != _REQUIRED_PARTITIONS:
        raise KafkaSetupError(f"topologia incompatível para {name}: esperado 3 partições")
    for partition in partitions.values():
        replicas = getattr(partition, "replicas", ())
        if len(replicas) != 1:
            raise KafkaSetupError(
                f"topologia incompatível para {name}: esperado fator de replicação 1"
            )


def _verify_topics(metadata: object) -> None:
    topics = getattr(metadata, "topics", {})
    for name in TOPICS:
        _verify_topic(name, topics.get(name))


def ensure_topics(
    bootstrap_servers: str,
    *,
    admin: object | None = None,
    clock: Any = time.monotonic,
) -> None:
    """Cria apenas tópicos ausentes e confere a topologia sem mutações destrutivas."""
    deadline = clock() + _TOPIC_DEADLINE_SECONDS
    try:
        if admin is None:
            if AdminClient is None:
                raise KafkaSetupError("confluent-kafka não está instalado")
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})

        def metadata_snapshot() -> object:
            try:
                return admin.list_topics(timeout=_remaining(deadline, clock, "startup Kafka"))
            except KafkaSetupError:
                raise
            except Exception as exc:
                raise KafkaSetupError("falha ao consultar metadata Kafka") from exc

        metadata = metadata_snapshot()
        current_topics = getattr(metadata, "topics", {})
        # Falha antes de criar qualquer outro tópico quando um tópico existente é incompatível.
        for name in TOPICS:
            existing = current_topics.get(name)
            if existing is not None:
                _verify_topic(name, existing)

        missing = [name for name in TOPICS if name not in current_topics]
        if missing:
            if NewTopic is None:
                raise KafkaSetupError("confluent-kafka não está instalado")
            specifications = [
                NewTopic(name, num_partitions=_REQUIRED_PARTITIONS, replication_factor=1)
                for name in missing
            ]
            try:
                futures = admin.create_topics(
                    specifications,
                    request_timeout=_remaining(deadline, clock, "startup Kafka"),
                )
            except Exception as exc:
                if not _topic_already_exists(exc):
                    raise KafkaSetupError("falha ao solicitar criação de tópicos") from exc
                futures = {}

            for name, future in futures.items():
                try:
                    future.result(timeout=_remaining(deadline, clock, "startup Kafka"))
                except KafkaSetupError:
                    raise
                except Exception as exc:
                    if _topic_already_exists(exc):
                        continue
                    raise KafkaSetupError(f"falha ao criar o tópico {name}") from exc

            _verify_topics(metadata_snapshot())
    except KafkaSetupError:
        raise
    except Exception as exc:
        raise KafkaSetupError("falha ao preparar tópicos Kafka") from exc


class KafkaPublisher:
    """Mantém um produtor e contabiliza callbacks individualmente por ciclo."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        producer: object | None = None,
        clock: Any = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        self._clock = clock
        self._logger = logging.getLogger(__name__) if logger is None else logger
        if producer is None:
            if Producer is None:
                raise KafkaPublishError("confluent-kafka não está instalado")
            config = {"bootstrap.servers": bootstrap_servers, **_PRODUCER_CONFIG}
            producer = Producer(config)
        self._producer = producer
        self._closed = False
        self._accepted = 0
        self._callbacks = 0
        self._delivered = 0
        self._pending = 0
        self._batch_error: str | None = None

    def _mark_failure(self, reason: str) -> None:
        if self._batch_error is None:
            self._batch_error = reason

    @staticmethod
    def _error_code(error: object) -> object:
        code = getattr(error, "code", None)
        if callable(code):
            try:
                return code()
            except Exception:
                return type(error).__name__
        return type(error).__name__

    def _on_delivery(
        self,
        error: object | None,
        event_id: str,
        location_id: str,
    ) -> None:
        self._callbacks += 1
        if self._pending:
            self._pending -= 1
        if error is not None:
            self._mark_failure("delivery callback returned an error")
            emit_json_log(
                self._logger,
                "delivery",
                "error",
                event_id=event_id,
                location_id=location_id,
                error_code=self._error_code(error),
            )
            return
        self._delivered += 1
        emit_json_log(
            self._logger,
            "delivery",
            "success",
            event_id=event_id,
            location_id=location_id,
        )

    def enqueue(self, event: WeatherEvent) -> None:
        """Enfileira uma ocorrência com prazo total de cinco segundos para fila cheia."""
        if self._closed:
            raise KafkaPublishError("publisher já foi fechado")
        key, value, timestamp_ms = serialize_event(event)
        event_id = (
            f"{event.location_id}:"
            f"{event.event_timestamp.astimezone(UTC).isoformat().replace('+00:00', 'Z')}"
        )
        deadline = self._clock() + _QUEUE_DEADLINE_SECONDS
        attempted = False
        while True:
            if attempted and self._clock() >= deadline:
                self._mark_failure("fila Kafka permaneceu cheia até o prazo")
                raise KafkaPublishError(
                    "prazo de cinco segundos excedido para fila Kafka",
                    delivered_count=self._delivered,
                )
            attempted = True
            try:
                self._producer.produce(
                    RAW_TOPIC,
                    key=key,
                    value=value,
                    timestamp=timestamp_ms,
                    on_delivery=lambda error, message: self._on_delivery(
                        error, event_id, event.location_id
                    ),
                )
                self._accepted += 1
                self._pending += 1
                return
            except BufferError as exc:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    self._mark_failure("fila Kafka permaneceu cheia até o prazo")
                    raise KafkaPublishError(
                        "prazo de cinco segundos excedido para fila Kafka",
                        delivered_count=self._delivered,
                    ) from exc
                try:
                    self._producer.poll(min(remaining, 0.1))
                except Exception as poll_error:
                    self._mark_failure("falha ao atender callbacks Kafka")
                    raise KafkaPublishError(
                        "falha ao atender callbacks Kafka",
                        delivered_count=self._delivered,
                    ) from poll_error
            except KafkaPublishError:
                raise
            except Exception as exc:
                self._mark_failure("falha ao enfileirar mensagem Kafka")
                raise KafkaPublishError(
                    "falha ao enfileirar mensagem Kafka",
                    delivered_count=self._delivered,
                ) from exc
    def _flush(self) -> int:
        try:
            outstanding = self._producer.flush(_FLUSH_TIMEOUT_SECONDS)
        except Exception as exc:
            self._mark_failure("falha durante flush Kafka")
            raise KafkaPublishError(
                "falha durante flush Kafka",
                delivered_count=self._delivered,
            ) from exc

        if outstanding:
            self._mark_failure("há mensagens pendentes após flush Kafka")
        if self._pending or self._callbacks != self._accepted:
            self._mark_failure("callback de entrega ausente ou mensagem pendente")
        if self._batch_error is not None:
            raise KafkaPublishError(
                self._batch_error,
                delivered_count=self._delivered,
            )
        delivered = self._delivered
        self._accepted = 0
        self._callbacks = 0
        self._delivered = 0
        self._pending = 0
        return delivered
    def finish_batch(self) -> int:
        """Confirma o ciclo e retorna a contagem de callbacks de entrega bem-sucedidos."""
        if self._closed:
            raise KafkaPublishError("publisher já foi fechado")
        if self._accepted == 0:
            if self._batch_error is not None:
                raise KafkaPublishError(self._batch_error, delivered_count=self._delivered)
            return 0
        return self._flush()

    def close(self) -> None:
        """Fecha o publisher após flush final limitado a doze segundos."""
        if self._closed:
            return
        self._closed = True
        self._flush()
