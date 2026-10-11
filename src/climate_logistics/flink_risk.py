"""PyFlink adapter that appends risk assessment to window metric JSON."""

from __future__ import annotations

import json

from pyflink.common import Types
from pyflink.datastream import DataStream
from pyflink.datastream.functions import MapFunction

from climate_logistics.risk_engine import enrich_metric


class AssessWindowRisk(MapFunction):
    """Enrich each valid aggregate and count only successfully serialized outputs."""

    def __init__(self) -> None:
        self._assessed_windows = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group().add_group("risk_engine")
        self._assessed_windows = metrics.counter("assessed_windows")

    def map(self, raw: str) -> str:
        metric = json.loads(raw)
        assessed = enrich_metric(metric)
        serialized = json.dumps(
            assessed,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        self._assessed_windows.inc()
        return serialized


def attach_risk_assessment(stream: DataStream) -> DataStream:
    """Add a stateless string transformation for each temporal metric record."""
    return stream.map(
        AssessWindowRisk(),
        output_type=Types.STRING(),
    ).name("weather-risk-assessment").uid("weather-risk-assessment")
