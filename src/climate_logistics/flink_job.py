"""PyFlink DataStream job for the first Kafka-to-diagnostic ingestion path."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import FlatMapFunction, MapFunction

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


@dataclass(frozen=True)
class JobSettings:
    """Parâmetros seguros da fonte Kafka e do modo limitado de QA."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    group_id: str = DEFAULT_GROUP_ID
    bounded: bool = False


class _ParseWeatherEvent(FlatMapFunction):
    def __init__(self) -> None:
        self._parsed_records = None
        self._invalid_records = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("weather_ingestion")
        self._parsed_records = metrics.counter("parsed_records")
        self._invalid_records = metrics.counter("invalid_records")

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

        values = event_values(event)
        self._parsed_records.inc()
        yield Row(*values)


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
    """Constrói a fonte, parsing tipado e saída diagnóstica sem estado temporal."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.bootstrap_servers)
        .set_topics(RAW_TOPIC)
        .set_group_id(settings.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
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
        _ParseWeatherEvent(),
        output_type=_row_type(),
    ).name("weather-json-parser").uid("weather-json-parser")
    diagnostic_stream = parsed_stream.map(
        _DiagnosticSummary(),
        output_type=Types.STRING(),
    ).name("weather-parse-diagnostic").uid("weather-parse-diagnostic")
    diagnostic_stream.print("weather-ingestion").name("weather-ingestion").uid(
        "weather-ingestion"
    )
    return env


def execute(job_name: str, settings: JobSettings | None = None) -> str:
    """Submete o job e escreve o ID real para automação de QA/operação."""
    result = build_pipeline(settings or JobSettings()).execute(job_name)
    job_id = str(result.get_job_id())
    print(f"JOB_ID={job_id}", flush=True)
    return job_id


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bounded", action="store_true", help="para nos offsets atuais do tópico")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    settings = JobSettings(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        bounded=args.bounded,
    )
    execute(args.job_name, settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())