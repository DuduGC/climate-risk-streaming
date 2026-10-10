"""PyFlink DataStream job for the first Kafka-to-diagnostic ingestion path."""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from pyflink.common import Configuration, Row, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import FlatMapFunction, KeyedProcessFunction, MapFunction
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig

from climate_logistics.data_quality import (
    DEFAULT_DEDUP_TTL_SECONDS,
    DataQualityError,
    event_identity,
    validate_event_time,
)
from climate_logistics.flink_temporal import (
    attach_temporal_metrics,
    configure_checkpointing,
)
from climate_logistics.weather_parser import (
    WEATHER_FIELDS,
    WEATHER_KINDS,
    WeatherParseError,
    diagnostic_summary,
    event_values,
    parse_weather_event,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_JOB_NAME = "weather-kafka-ingestion"
DEFAULT_GROUP_ID = "weather-flink-ingestion"
RAW_TOPIC = "weather.raw"
DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"
QA_JOB_PREFIX = "weather-flink-qa-"
_LOCATION_INDEX = WEATHER_FIELDS.index("location_id")
_EVENT_TIMESTAMP_INDEX = WEATHER_FIELDS.index("event_timestamp")


@dataclass(frozen=True)
class JobSettings:
    """Parâmetros seguros da fonte Kafka e do modo limitado de QA."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    group_id: str = DEFAULT_GROUP_ID
    bounded: bool = False
    dedup_ttl_seconds: int = DEFAULT_DEDUP_TTL_SECONDS
    qa_audit: bool = False
    checkpoint_interval_seconds: int = 60

    def __post_init__(self) -> None:
        if isinstance(self.dedup_ttl_seconds, bool) or not isinstance(
            self.dedup_ttl_seconds, int
        ):
            raise ValueError("dedup_ttl_seconds deve ser um inteiro")
        if self.dedup_ttl_seconds != DEFAULT_DEDUP_TTL_SECONDS:
            if not self.qa_audit:
                raise ValueError("somente QA pode alterar o TTL operacional de deduplicação")
            if not 2 <= self.dedup_ttl_seconds <= 60:
                raise ValueError("TTL de deduplicação QA deve estar entre 2 e 60 segundos")
        if isinstance(self.checkpoint_interval_seconds, bool) or not isinstance(
            self.checkpoint_interval_seconds, int
        ):
            raise ValueError("checkpoint_interval_seconds deve ser um inteiro")
        if not 5 <= self.checkpoint_interval_seconds <= 60:
            raise ValueError(
                "checkpoint_interval_seconds deve estar entre 5 e 60 segundos"
            )
        if self.checkpoint_interval_seconds != 60 and not self.qa_audit:
            raise ValueError(
                "somente QA pode alterar o intervalo operacional de checkpoint"
            )


class _ParseWeatherEvent(FlatMapFunction):
    def __init__(
        self,
        *,
        observed_at: datetime | None = None,
        qa_audit: bool = False,
    ) -> None:
        self._parsed_records = None
        self._invalid_records = None
        self._temporal_rejected_records = None
        self._observed_at = observed_at
        self._qa_audit = qa_audit

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group()
        ingestion_metrics = metrics.add_group("weather_ingestion")
        self._parsed_records = ingestion_metrics.counter("parsed_records")
        self._invalid_records = ingestion_metrics.counter("invalid_records")
        quality_metrics = metrics.add_group("data_quality")
        self._temporal_rejected_records = quality_metrics.counter(
            "temporal_rejected_records"
        )

    def flat_map(self, raw: str):
        try:
            event = parse_weather_event(raw)
        except WeatherParseError:
            self._invalid_records.inc()
            LOGGER.warning(
                json.dumps(
                    {
                        "component": "flink",
                        "operation": "parse",
                        "status": "error",
                        "reason": "weather_parse_error",
                        "metric": "invalid_records",
                    },
                    separators=(",", ":"),
                )
            )
            return

        self._parsed_records.inc()
        observed_at = self._observed_at if self._observed_at is not None else datetime.now(UTC)
        try:
            validate_event_time(event, observed_at=observed_at)
        except DataQualityError as exc:
            self._invalid_records.inc()
            self._temporal_rejected_records.inc()
            location_id, timestamp = event_identity(event)
            if self._qa_audit:
                _quality_audit(
                    status="rejected",
                    reason=exc.reason,
                    location_id=location_id,
                    event_timestamp=timestamp,
                )
            else:
                LOGGER.warning(
                    json.dumps(
                        {
                            "component": "flink",
                            "operation": "data_quality",
                            "status": "rejected",
                            "reason": exc.reason,
                            "metric": "temporal_rejected_records",
                        },
                        separators=(",", ":"),
                    )
                )
            return

        values = event_values(event)
        yield Row(*values)


def _quality_audit(
    *,
    status: str,
    location_id: str,
    event_timestamp: str,
    reason: str | None = None,
) -> None:
    record = {
        "component": "flink",
        "operation": "data_quality",
        "status": status,
        "location_id": location_id,
        "event_timestamp": event_timestamp,
    }
    if reason is not None:
        record["reason"] = reason
    LOGGER.info(json.dumps(record, separators=(",", ":")))


class _DeduplicateWeather(KeyedProcessFunction):
    """Emite a primeira ocorrência de cada instante UTC por localidade."""

    def __init__(self, *, ttl_seconds: int = DEFAULT_DEDUP_TTL_SECONDS, qa_audit: bool = False):
        self._ttl_seconds = ttl_seconds
        self._qa_audit = qa_audit
        self._seen = None
        self._duplicate_records = None
        self._accepted_records = None

    def open(self, runtime_context) -> None:
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(self._ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_incrementally(10, True)
            .build()
        )
        descriptor = MapStateDescriptor(
            "weather_seen_event_times",
            Types.STRING(),
            Types.BOOLEAN(),
        )
        descriptor.enable_time_to_live(ttl_config)
        self._seen = runtime_context.get_map_state(descriptor)
        quality_metrics = runtime_context.get_metrics_group().add_group("data_quality")
        self._duplicate_records = quality_metrics.counter("duplicate_records")
        self._accepted_records = quality_metrics.counter("accepted_records")

    def process_element(self, value: Row, ctx):
        location_id = str(value[_LOCATION_INDEX])
        timestamp = str(value[_EVENT_TIMESTAMP_INDEX])
        if self._seen.get(timestamp) is not None:
            self._duplicate_records.inc()
            if self._qa_audit:
                _quality_audit(
                    status="duplicate",
                    reason="duplicate_identity",
                    location_id=location_id,
                    event_timestamp=timestamp,
                )
            return

        self._seen.put(timestamp, True)
        self._accepted_records.inc()
        if self._qa_audit:
            _quality_audit(
                status="accepted",
                location_id=location_id,
                event_timestamp=timestamp,
            )
        yield value


class _DiagnosticSummary(MapFunction):
    def map(self, row: Row) -> str:
        return diagnostic_summary(tuple(row))


def _row_type():
    factories = {
        "STRING": Types.STRING,
        "DOUBLE": Types.DOUBLE,
        "INT": Types.INT,
    }
    field_types = [factories[kind]() for kind in WEATHER_KINDS]
    return Types.ROW_NAMED(list(WEATHER_FIELDS), field_types)


def build_pipeline(settings: JobSettings) -> StreamExecutionEnvironment:
    """Constrói a fonte, validação temporal, deduplicação e saída diagnóstica."""
    env = StreamExecutionEnvironment.get_execution_environment()
    qa_execution = settings.qa_audit or settings.dedup_ttl_seconds != DEFAULT_DEDUP_TTL_SECONDS
    execution_configuration = Configuration()
    # PyFlink 1.20.3's remote MapState read cache can return an expired marker.
    execution_configuration.set_integer("python.state.cache-size", 0)
    if qa_execution:
        execution_configuration.set_integer("python.fn-execution.bundle.size", 1)
        execution_configuration.set_integer("python.fn-execution.bundle.time", 1000)
    env.configure(execution_configuration)
    configure_checkpointing(
        env, interval_seconds=settings.checkpoint_interval_seconds
    )
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(200)

    starting_offsets = (
        KafkaOffsetsInitializer.latest()
        if qa_execution and not settings.bounded
        else KafkaOffsetsInitializer.earliest()
    )
    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.bootstrap_servers)
        .set_topics(RAW_TOPIC)
        .set_group_id(settings.group_id)
        .set_starting_offsets(starting_offsets)
        .set_property("enable.auto.commit", "false")
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if settings.bounded:
        source_builder.set_bounded(KafkaOffsetsInitializer.latest())
    source = source_builder.build()

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "weather-raw-source",
    ).uid("weather-raw-source")
    parsed_stream = raw_stream.flat_map(
        _ParseWeatherEvent(qa_audit=settings.qa_audit),
        output_type=_row_type(),
    ).name("weather-json-parser").uid("weather-json-parser")
    deduplicated_stream = parsed_stream.key_by(
        lambda row: row[_LOCATION_INDEX],
        key_type=Types.STRING(),
    ).process(
        _DeduplicateWeather(
            ttl_seconds=settings.dedup_ttl_seconds,
            qa_audit=settings.qa_audit,
        ),
        output_type=_row_type(),
    ).name("weather-event-deduplication").uid("weather-event-deduplication")
    diagnostic_stream = deduplicated_stream.map(
        _DiagnosticSummary(),
        output_type=Types.STRING(),
    ).name("weather-parse-diagnostic").uid("weather-parse-diagnostic")
    diagnostic_stream.print("weather-ingestion").name("weather-ingestion").uid(
        "weather-ingestion"
    )
    temporal_stream = attach_temporal_metrics(deduplicated_stream)
    temporal_stream.print("weather-metrics").name("weather-metrics").uid(
        "weather-metrics"
    )
    return env


def execute(job_name: str, settings: JobSettings | None = None) -> str:
    """Submete o job e escreve o ID real para automação de QA/operação."""
    effective_settings = settings or JobSettings()
    _validate_execution_identity(job_name, effective_settings)
    result = build_pipeline(effective_settings).execute(job_name)
    job_id = str(result.get_job_id())
    print(f"JOB_ID={job_id}", flush=True)
    return job_id


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bounded", action="store_true", help="para nos offsets atuais do tópico")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument(
        "--qa-dedup-ttl-seconds",
        type=int,
        choices=range(2, 61),
        help="TTL curto exclusivo de QA (2 a 60 segundos)",
    )
    parser.add_argument(
        "--qa-audit",
        action="store_true",
        help="emite auditoria mínima de aceitos, rejeitados e duplicados para QA",
    )
    parser.add_argument(
        "--qa-checkpoint-interval-seconds",
        type=int,
        choices=range(5, 61),
        help="intervalo reduzido de checkpoint exclusivo de QA (5 a 60 segundos)",
    )
    return parser.parse_args(argv)


def _validate_execution_identity(
    job_name: str,
    settings: JobSettings,
    *,
    group_id: str | None = None,
    checkpoint_interval_override: bool = False,
) -> None:
    if checkpoint_interval_override and not settings.qa_audit:
        raise ValueError(
            "--qa-checkpoint-interval-seconds exige modo QA com --qa-audit"
        )

    if (
        not settings.qa_audit
        and settings.dedup_ttl_seconds == DEFAULT_DEDUP_TTL_SECONDS
        and settings.checkpoint_interval_seconds == 60
        and not checkpoint_interval_override
    ):
        return

    effective_group_id = settings.group_id if group_id is None else group_id
    suffix = job_name.removeprefix(QA_JOB_PREFIX)
    if (
        not job_name.startswith(QA_JOB_PREFIX)
        or re.fullmatch(r"[A-Za-z0-9]+", suffix) is None
        or effective_group_id not in {job_name, f"{job_name}-group"}
    ):
        raise ValueError(
            "auditoria ou TTL curto exige job-name weather-flink-qa-<id> "
            "e group-id com o mesmo prefixo exclusivo"
        )


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    checkpoint_interval_override = args.qa_checkpoint_interval_seconds is not None
    settings = JobSettings(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        bounded=args.bounded,
        dedup_ttl_seconds=(
            DEFAULT_DEDUP_TTL_SECONDS
            if args.qa_dedup_ttl_seconds is None
            else args.qa_dedup_ttl_seconds
        ),
        qa_audit=args.qa_audit,
        checkpoint_interval_seconds=(
            60
            if args.qa_checkpoint_interval_seconds is None
            else args.qa_checkpoint_interval_seconds
        ),
    )
    _validate_execution_identity(
        args.job_name,
        settings,
        checkpoint_interval_override=checkpoint_interval_override,
    )
    execute(args.job_name, settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
