"""Parameterized PostgreSQL statement for one validated business assessment."""

from __future__ import annotations

BUSINESS_UPSERT_SQL = """
WITH incoming AS (
    SELECT ?::jsonb AS assessment, ?::text AS alert_id
), upserted_metric AS (
    INSERT INTO climate.weather_metrics (
        location_id,
        window_start_ms,
        window_end_ms,
        event_count,
        temperature_avg,
        precipitation_max,
        precipitation_rate_max_mm_per_hour,
        wind_speed_max,
        wind_gust_max,
        visibility_min,
        measurement_interval_min_seconds,
        measurement_interval_max_seconds,
        first_event_timestamp_ms,
        last_event_timestamp_ms,
        schema_version,
        risk_score,
        risk_level,
        risk_rules_version,
        risk_components
    )
    SELECT
        assessment->>'location_id',
        (assessment->>'window_start_ms')::bigint,
        (assessment->>'window_end_ms')::bigint,
        (assessment->>'event_count')::bigint,
        (assessment->>'temperature_avg')::double precision,
        (assessment->>'precipitation_max')::double precision,
        (assessment->>'precipitation_rate_max_mm_per_hour')::double precision,
        (assessment->>'wind_speed_max')::double precision,
        (assessment->>'wind_gust_max')::double precision,
        (assessment->>'visibility_min')::double precision,
        (assessment->>'measurement_interval_min_seconds')::integer,
        (assessment->>'measurement_interval_max_seconds')::integer,
        (assessment->>'first_event_timestamp_ms')::bigint,
        (assessment->>'last_event_timestamp_ms')::bigint,
        (assessment->>'schema_version')::smallint,
        (assessment->>'risk_score')::smallint,
        assessment->>'risk_level',
        assessment->>'risk_rules_version',
        assessment->'risk_components'
    FROM incoming
    ON CONFLICT (location_id, window_start_ms, window_end_ms, risk_rules_version)
    DO UPDATE SET
        event_count = EXCLUDED.event_count,
        temperature_avg = EXCLUDED.temperature_avg,
        precipitation_max = EXCLUDED.precipitation_max,
        precipitation_rate_max_mm_per_hour = EXCLUDED.precipitation_rate_max_mm_per_hour,
        wind_speed_max = EXCLUDED.wind_speed_max,
        wind_gust_max = EXCLUDED.wind_gust_max,
        visibility_min = EXCLUDED.visibility_min,
        measurement_interval_min_seconds = EXCLUDED.measurement_interval_min_seconds,
        measurement_interval_max_seconds = EXCLUDED.measurement_interval_max_seconds,
        first_event_timestamp_ms = EXCLUDED.first_event_timestamp_ms,
        last_event_timestamp_ms = EXCLUDED.last_event_timestamp_ms,
        schema_version = EXCLUDED.schema_version,
        risk_score = EXCLUDED.risk_score,
        risk_level = EXCLUDED.risk_level,
        risk_components = EXCLUDED.risk_components
    RETURNING
        location_id,
        window_start_ms,
        window_end_ms,
        risk_score,
        risk_level,
        risk_rules_version
), alert_payload AS (
    SELECT
        metric.location_id,
        metric.window_start_ms,
        metric.window_end_ms,
        metric.risk_rules_version,
        metric.risk_score,
        metric.risk_level,
        incoming.alert_id,
        incoming.assessment || jsonb_build_object(
            'operation', 'weather_alert',
            'alert_id', incoming.alert_id
        ) AS payload
    FROM upserted_metric AS metric
    CROSS JOIN incoming
    WHERE metric.risk_score >= 50
)
INSERT INTO climate.weather_alerts (
    alert_id,
    location_id,
    window_start_ms,
    window_end_ms,
    risk_rules_version,
    risk_score,
    risk_level,
    payload
)
SELECT
    alert_id,
    location_id,
    window_start_ms,
    window_end_ms,
    risk_rules_version,
    risk_score,
    risk_level,
    payload
FROM alert_payload
ON CONFLICT (alert_id)
DO UPDATE SET
    location_id = EXCLUDED.location_id,
    window_start_ms = EXCLUDED.window_start_ms,
    window_end_ms = EXCLUDED.window_end_ms,
    risk_rules_version = EXCLUDED.risk_rules_version,
    risk_score = EXCLUDED.risk_score,
    risk_level = EXCLUDED.risk_level,
    payload = EXCLUDED.payload
""".strip()
