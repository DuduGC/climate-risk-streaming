from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from climate_logistics.business_records import jdbc_values
from climate_logistics.business_sql import BUSINESS_UPSERT_SQL

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def _assessment(
    *,
    rate: float = 20.0,
    score: int = 50,
    level: str = "HIGH",
    location_id: str = "RJ01",
    time_shift_ms: int = 0,
) -> dict[str, object]:
    return {
        "component": "flink",
        "operation": "risk_assessment",
        "schema_version": 1,
        "location_id": location_id,
        "window_start_ms": 1_791_633_600_000 + time_shift_ms,
        "window_end_ms": 1_791_635_400_000 + time_shift_ms,
        "event_count": 3,
        "temperature_avg": 22.5,
        "precipitation_max": 5.0,
        "precipitation_rate_max_mm_per_hour": rate,
        "wind_speed_max": 0.0,
        "wind_gust_max": 0.0,
        "visibility_min": 10_000.0,
        "measurement_interval_min_seconds": 300,
        "measurement_interval_max_seconds": 900,
        "first_event_timestamp_ms": 1_791_633_620_000 + time_shift_ms,
        "last_event_timestamp_ms": 1_791_634_900_000 + time_shift_ms,
        "risk_score": score,
        "risk_level": level,
        "risk_components": {
            "precipitation_rate": score,
            "wind_speed": 0,
            "wind_gust": 0,
            "visibility": 0,
        },
        "risk_rules_version": "mvp-1",
    }


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _integration_sql() -> str:
    statement = BUSINESS_UPSERT_SQL
    assert statement.count("?") == 2
    return statement.replace("?", "$1", 1).replace("?", "$2", 1)


def _psql_as_app(script: str) -> subprocess.CompletedProcess[str]:
    if os.environ.get("POSTGRES_BUSINESS_TEST") != "1":
        pytest.skip("set POSTGRES_BUSINESS_TEST=1 to opt in to PostgreSQL integration")

    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("docker is required when PostgreSQL integration is enabled", pytrace=False)
    container_command = (
        'export PGUSER="$APP_DB_USER" PGPASSWORD="$APP_DB_PASSWORD" '
        'PGDATABASE="$POSTGRES_DB" PGHOST=127.0.0.1; '
        'exec psql --no-psqlrc --quiet --set=ON_ERROR_STOP=1 '
        '--set=VERBOSITY=verbose'
    )
    return subprocess.run(
        [docker, "compose", "exec", "-T", "postgres", "bash", "-c", container_command],
        cwd=ROOT,
        input=script,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        timeout=30,
    )


def test_postgres_upsert_replay_divergence_and_statement_atomicity():
    assessment = _assessment()
    divergent = _assessment(rate=30.0, score=75, level="CRITICAL")
    atomic_failure = _assessment(time_shift_ms=3_600_000)
    assessment_json, expected_alert_id = jdbc_values(assessment)
    divergent_json, divergent_alert_id = jdbc_values(divergent)
    atomic_json, _ = jdbc_values(atomic_failure)
    assert expected_alert_id == divergent_alert_id

    query = _sql_literal(_integration_sql())
    sql = f"""
BEGIN;
DO $stage8$
DECLARE
    v_upsert CONSTANT text := {query};
    v_assessment jsonb := {_sql_literal(assessment_json)}::jsonb;
    v_divergent jsonb := {_sql_literal(divergent_json)}::jsonb;
    v_atomic_failure jsonb := {_sql_literal(atomic_json)}::jsonb;
    v_alert_id text := {_sql_literal(expected_alert_id)};
    v_metric_count bigint;
    v_alert_count bigint;
    v_score integer;
    v_payload jsonb;
BEGIN
    EXECUTE v_upsert USING v_assessment, v_alert_id;
    EXECUTE v_upsert USING v_assessment, v_alert_id;

    SELECT count(*) INTO v_metric_count
    FROM climate.weather_metrics
    WHERE location_id = 'RJ01'
      AND window_start_ms = 1791633600000
      AND window_end_ms = 1791635400000
      AND risk_rules_version = 'mvp-1';
    SELECT count(*) INTO v_alert_count
    FROM climate.weather_alerts
    WHERE alert_id = v_alert_id;
    IF v_metric_count <> 1 OR v_alert_count <> 1 THEN
        RAISE EXCEPTION 'idempotent replay changed row counts';
    END IF;

    SELECT risk_score INTO v_score
    FROM climate.weather_metrics
    WHERE location_id = 'RJ01'
      AND window_start_ms = 1791633600000
      AND window_end_ms = 1791635400000
      AND risk_rules_version = 'mvp-1';
    SELECT payload INTO v_payload
    FROM climate.weather_alerts
    WHERE alert_id = v_alert_id;
    IF v_score <> 50
       OR v_payload->>'operation' IS DISTINCT FROM 'weather_alert'
       OR v_payload->>'risk_score' IS DISTINCT FROM '50'
       OR v_payload->>'alert_id' IS DISTINCT FROM v_alert_id THEN
        RAISE EXCEPTION 'persisted assessment and alert payload diverged';
    END IF;

    BEGIN
        EXECUTE v_upsert USING v_divergent, v_alert_id;
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'divergent identity was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        EXECUTE v_upsert USING v_atomic_failure, 'invalid-alert-id';
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'invalid alert id was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    SELECT count(*) INTO v_metric_count
    FROM climate.weather_metrics
    WHERE location_id = 'RJ01'
      AND window_start_ms = 1791633600000
      AND window_end_ms = 1791635400000
      AND risk_rules_version = 'mvp-1';
    SELECT count(*) INTO v_alert_count
    FROM climate.weather_alerts
    WHERE alert_id = v_alert_id;
    IF v_metric_count <> 1 OR v_alert_count <> 1 THEN
        RAISE EXCEPTION 'failed statement changed existing rows';
    END IF;
    SELECT count(*) INTO v_metric_count
    FROM climate.weather_metrics
    WHERE location_id = 'RJ01'
      AND window_start_ms = 1791637200000
      AND window_end_ms = 1791639000000
      AND risk_rules_version = 'mvp-1';
    IF v_metric_count <> 0 THEN
        RAISE EXCEPTION 'failed alert insert did not roll back its metric write';
    END IF;
END
$stage8$;
ROLLBACK;
"""
    result = _psql_as_app(sql)

    assert result.returncode == 0, result.stderr


def test_postgres_application_role_has_only_minimum_business_privileges():
    sql = """
DO $stage8$
DECLARE
    v_superuser boolean;
    v_createdb boolean;
    v_createrole boolean;
    v_replication boolean;
    v_inherit boolean;
    v_bypassrls boolean;
    v_database_owner text;
    v_schema_owner text;
BEGIN
    SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolinherit, rolbypassrls
    INTO v_superuser, v_createdb, v_createrole, v_replication, v_inherit, v_bypassrls
    FROM pg_roles WHERE rolname = current_user;
    IF v_superuser OR v_createdb OR v_createrole OR v_replication OR v_inherit OR v_bypassrls THEN
        RAISE EXCEPTION 'application role has administrative attributes';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members
        WHERE member = (SELECT oid FROM pg_roles WHERE rolname = current_user)
    ) THEN
        RAISE EXCEPTION 'application role inherits another role';
    END IF;
    SELECT pg_get_userbyid(datdba) INTO v_database_owner
    FROM pg_database WHERE datname = current_database();
    SELECT pg_get_userbyid(nspowner) INTO v_schema_owner
    FROM pg_namespace WHERE nspname = 'climate';
    IF v_database_owner = current_user OR v_schema_owner = current_user THEN
        RAISE EXCEPTION 'application role owns a business database object';
    END IF;
    IF NOT has_schema_privilege(current_user, 'climate', 'USAGE')
       OR has_schema_privilege(current_user, 'climate', 'CREATE')
       OR has_schema_privilege(current_user, 'public', 'CREATE')
       OR has_database_privilege(current_user, current_database(), 'CREATE')
       OR has_database_privilege(current_user, current_database(), 'TEMPORARY') THEN
        RAISE EXCEPTION 'application role has unexpected schema or database privilege';
    END IF;
    IF NOT has_table_privilege(current_user, 'climate.locations', 'SELECT')
       OR has_table_privilege(current_user, 'climate.locations', 'INSERT')
       OR has_table_privilege(current_user, 'climate.locations', 'UPDATE')
       OR has_table_privilege(current_user, 'climate.locations', 'DELETE') THEN
        RAISE EXCEPTION 'application dimension grants are not minimal';
    END IF;
    IF NOT has_table_privilege(current_user, 'climate.weather_metrics', 'SELECT')
       OR NOT has_table_privilege(current_user, 'climate.weather_metrics', 'INSERT')
       OR NOT has_table_privilege(current_user, 'climate.weather_metrics', 'UPDATE')
       OR has_table_privilege(current_user, 'climate.weather_metrics', 'DELETE')
       OR has_table_privilege(current_user, 'climate.weather_metrics', 'TRUNCATE')
       OR NOT has_table_privilege(current_user, 'climate.weather_alerts', 'SELECT')
       OR NOT has_table_privilege(current_user, 'climate.weather_alerts', 'INSERT')
       OR NOT has_table_privilege(current_user, 'climate.weather_alerts', 'UPDATE')
       OR has_table_privilege(current_user, 'climate.weather_alerts', 'DELETE')
       OR has_table_privilege(current_user, 'climate.weather_alerts', 'TRUNCATE') THEN
        RAISE EXCEPTION 'business fact grants are not minimal';
    END IF;

    BEGIN
        EXECUTE 'CREATE TEMP TABLE stage8_unexpected_temp (value integer)';
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'TEMP creation was authorized';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;
    BEGIN
        EXECUTE 'CREATE TABLE climate.stage8_unexpected_ddl (value integer)';
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'schema DDL was authorized';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;
END
$stage8$;
"""
    result = _psql_as_app(sql)

    assert result.returncode == 0, result.stderr


def test_postgres_rejects_null_wrong_type_and_incoherent_alert_payload_fields():
    assessment = _assessment()
    assessment_json, alert_id = jdbc_values(assessment)
    query = _sql_literal(_integration_sql())
    sql = f"""
BEGIN;
DO $stage8$
DECLARE
    v_upsert CONSTANT text := {query};
    v_assessment jsonb := {_sql_literal(assessment_json)}::jsonb;
    v_alert_id text := {_sql_literal(alert_id)};
    v_payload jsonb;
    v_bad_payload jsonb;
    v_sqlstate text;
    v_case record;
BEGIN
    EXECUTE v_upsert USING v_assessment, v_alert_id;
    SELECT payload INTO v_payload
    FROM climate.weather_alerts
    WHERE alert_id = v_alert_id;
    IF v_payload IS NULL THEN
        RAISE EXCEPTION 'valid alert was not written';
    END IF;

    FOR v_case IN
        SELECT * FROM (VALUES
            (ARRAY['component']::text[], 'null'::jsonb),
            (ARRAY['event_count']::text[], 'true'::jsonb),
            (ARRAY['temperature_avg']::text[], 'true'::jsonb),
            (ARRAY['temperature_avg']::text[], '1e400'::jsonb),
            (ARRAY['precipitation_max']::text[], '-1'::jsonb),
            (ARRAY['measurement_interval_min_seconds']::text[], '0'::jsonb),
            (ARRAY['last_event_timestamp_ms']::text[], '1791635400000'::jsonb),
            (ARRAY['risk_components', 'precipitation_rate']::text[], '75'::jsonb),
            (ARRAY['unexpected']::text[], '1'::jsonb)
        ) AS invalid_payload(path, replacement)
    LOOP
        v_bad_payload := jsonb_set(v_payload, v_case.path, v_case.replacement);
        BEGIN
            UPDATE climate.weather_alerts
            SET payload = v_bad_payload
            WHERE alert_id = v_alert_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'alert disappeared during payload validation';
            END IF;
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'invalid payload field was accepted';
        EXCEPTION WHEN check_violation THEN
            GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE;
            IF v_sqlstate IS DISTINCT FROM '23514' THEN
                RAISE;
            END IF;
        END;
    END LOOP;

    IF (SELECT payload FROM climate.weather_alerts WHERE alert_id = v_alert_id)
       IS DISTINCT FROM v_payload THEN
        RAISE EXCEPTION 'rejected payload updates changed the valid alert';
    END IF;
END
$stage8$;
ROLLBACK;
"""
    result = _psql_as_app(sql)

    assert result.returncode == 0, result.stderr
