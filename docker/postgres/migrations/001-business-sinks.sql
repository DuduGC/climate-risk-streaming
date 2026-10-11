\set ON_ERROR_STOP on
\getenv migration_admin POSTGRES_USER
\getenv migration_app APP_DB_USER
\getenv migration_database POSTGRES_DB

BEGIN;

SELECT current_user = :'migration_admin' AS stage8_is_admin \gset
\if :stage8_is_admin
\else
\echo 'business schema migration must run as POSTGRES_USER'
\quit 3
\endif

SELECT COALESCE(
    (
        SELECT rolcanlogin
           AND NOT rolsuper
           AND NOT rolcreatedb
           AND NOT rolcreaterole
           AND NOT rolreplication
           AND NOT rolbypassrls
           AND NOT rolinherit
           AND NOT EXISTS (
               SELECT 1 FROM pg_auth_members WHERE member = pg_roles.oid
           )
        FROM pg_roles
        WHERE rolname = :'migration_app'
    ),
    false
) AS stage8_app_is_minimal \gset
\if :stage8_app_is_minimal
\else
\echo 'application role is missing or has administrative privileges'
\quit 3
\endif

SELECT set_config('stage8.admin_role', :'migration_admin', true) AS stage8_ignored \gset
SELECT set_config('stage8.app_role', :'migration_app', true) AS stage8_ignored \gset
SELECT set_config('stage8.database', :'migration_database', true) AS stage8_ignored \gset

CREATE SCHEMA IF NOT EXISTS climate AUTHORIZATION :"migration_admin";

CREATE TEMP VIEW stage8_business_schema_fingerprint AS
WITH target_namespace AS (
    SELECT n.oid, n.nspname, n.nspowner, n.nspacl
    FROM pg_namespace AS n
    WHERE n.nspname = 'climate'
), target_relations AS (
    SELECT c.*
    FROM pg_class AS c
    JOIN target_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relname = ANY (
        ARRAY['locations', 'weather_metrics', 'weather_alerts']::name[]
    )
), parts AS (
    SELECT
        'schema:' || n.nspname || ':' || pg_get_userbyid(n.nspowner) AS part
    FROM target_namespace AS n
    UNION ALL
    SELECT
        'relation:' || c.relname || ':' || c.relkind::text || ':'
        || pg_get_userbyid(c.relowner) || ':' || c.relpersistence::text || ':'
        || c.relrowsecurity::text || ':' || c.relforcerowsecurity::text || ':'
        || COALESCE(c.reloptions::text, '') AS part
    FROM target_relations AS c
    UNION ALL
    SELECT
        'column:' || c.relname || ':' || a.attnum || ':' || a.attname || ':'
        || format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull || ':'
        || a.attidentity::text || ':' || a.attgenerated::text || ':'
        || COALESCE(a.attacl::text, '') || ':'
        || COALESCE(pg_get_expr(d.adbin, d.adrelid), '') AS part
    FROM target_relations AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    LEFT JOIN pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attnum > 0 AND NOT a.attisdropped
    UNION ALL
    SELECT
        'constraint:' || c.relname || ':' || con.conname || ':' || con.contype::text || ':'
        || con.condeferrable || ':' || con.condeferred || ':' || con.convalidated || ':'
        || pg_get_constraintdef(con.oid, true) AS part
    FROM target_relations AS c
    JOIN pg_constraint AS con ON con.conrelid = c.oid
    UNION ALL
    SELECT
        'index:' || c.relname || ':' || index_class.relname || ':'
        || idx.indisunique || ':' || idx.indisprimary || ':' || idx.indisvalid || ':'
        || idx.indisready || ':' || pg_get_indexdef(idx.indexrelid) AS part
    FROM target_relations AS c
    JOIN pg_index AS idx ON idx.indrelid = c.oid
    JOIN pg_class AS index_class ON index_class.oid = idx.indexrelid
    UNION ALL
    SELECT
        'trigger:' || c.relname || ':' || trg.tgname || ':' || trg.tgenabled::text || ':'
        || pg_get_triggerdef(trg.oid, true) || ':' || proc.prosrc || ':'
        || proc.prosecdef || ':' || COALESCE(proc.proconfig::text, '') AS part
    FROM target_relations AS c
    JOIN pg_trigger AS trg ON trg.tgrelid = c.oid AND NOT trg.tgisinternal
    JOIN pg_proc AS proc ON proc.oid = trg.tgfoid
    UNION ALL
    SELECT
        'policy:' || c.relname || ':' || row_policy.polname || ':'
        || row_policy.polcmd::text || ':' || row_policy.polpermissive::text || ':'
        || row_policy.polroles::text || ':'
        || COALESCE(pg_get_expr(row_policy.polqual, row_policy.polrelid), '') || ':'
        || COALESCE(pg_get_expr(row_policy.polwithcheck, row_policy.polrelid), '') AS part
    FROM target_relations AS c
    JOIN pg_policy AS row_policy ON row_policy.polrelid = c.oid
    UNION ALL
    SELECT
        'schema_acl:' || COALESCE(
            string_agg(
                COALESCE(grantee.rolname, 'PUBLIC') || ':'
                || acl.privilege_type || ':' || acl.is_grantable,
                ',' ORDER BY COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type
            ),
            ''
        ) AS part
    FROM target_namespace AS n
    CROSS JOIN LATERAL aclexplode(
        COALESCE(n.nspacl, acldefault('n', n.nspowner))
    ) AS acl
    LEFT JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
    UNION ALL
    SELECT
        'public_schema_acl:' || COALESCE(
            string_agg(
                COALESCE(grantee.rolname, 'PUBLIC') || ':'
                || acl.privilege_type || ':' || acl.is_grantable,
                ',' ORDER BY COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type
            ),
            ''
        ) AS part
    FROM pg_namespace AS public_namespace
    CROSS JOIN LATERAL aclexplode(
        COALESCE(
            public_namespace.nspacl,
            acldefault('n', public_namespace.nspowner)
        )
    ) AS acl
    LEFT JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
    WHERE public_namespace.nspname = 'public'
    UNION ALL
    SELECT
        'table_acl:' || c.relname || ':' || COALESCE(
            (
                SELECT string_agg(
                    COALESCE(grantee.rolname, 'PUBLIC') || ':'
                    || acl.privilege_type || ':' || acl.is_grantable,
                    ',' ORDER BY COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type
                )
                FROM aclexplode(
                    COALESCE(c.relacl, acldefault('r', c.relowner))
                ) AS acl
                LEFT JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
            ),
            ''
        ) AS part
    FROM target_relations AS c
    UNION ALL
    SELECT
        'database_owner:' || pg_get_userbyid(db.datdba) AS part
    FROM pg_database AS db
    WHERE db.datname = current_database()
    UNION ALL
    SELECT
        'database_acl:' || COALESCE(
            string_agg(
                COALESCE(grantee.rolname, 'PUBLIC') || ':'
                || acl.privilege_type || ':' || acl.is_grantable,
                ',' ORDER BY COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type
            ),
            ''
        ) AS part
    FROM pg_database AS db
    CROSS JOIN LATERAL aclexplode(
        COALESCE(db.datacl, acldefault('d', db.datdba))
    ) AS acl
    LEFT JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
    WHERE db.datname = current_database()
    UNION ALL
    SELECT
        'app_role:' || app_role_row.rolname || ':' || app_role_row.rolcanlogin || ':'
        || app_role_row.rolsuper || ':' || app_role_row.rolcreatedb || ':'
        || app_role_row.rolcreaterole || ':' || app_role_row.rolreplication || ':'
        || app_role_row.rolinherit || ':'
        || app_role_row.rolbypassrls || ':' || app_role_row.rolconnlimit
        AS part
    FROM pg_roles AS app_role_row
    WHERE app_role_row.rolname = current_setting('stage8.app_role', true)
    UNION ALL
    SELECT
        'app_membership:' || granted_role.rolname || ':' || member_role.rolname || ':'
        || membership.admin_option AS part
    FROM pg_auth_members AS membership
    JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid
    JOIN pg_roles AS member_role ON member_role.oid = membership.member
    WHERE member_role.rolname = current_setting('stage8.app_role', true)
    UNION ALL
    SELECT
        'trigger_function:' || proc.proname || ':' || pg_get_userbyid(proc.proowner) || ':'
        || proc.prosrc || ':' || proc.prosecdef || ':' || proc.provolatile::text || ':'
        || proc.proparallel::text || ':' || COALESCE(proc.proconfig::text, '') || ':'
        || COALESCE(
            (
                SELECT string_agg(
                    COALESCE(grantee.rolname, 'PUBLIC') || ':'
                    || acl.privilege_type || ':' || acl.is_grantable,
                    ',' ORDER BY COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type
                )
                FROM aclexplode(
                    COALESCE(proc.proacl, acldefault('f', proc.proowner))
                ) AS acl
                LEFT JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
            ),
            ''
        ) AS part
    FROM pg_proc AS proc
    JOIN pg_namespace AS function_namespace ON function_namespace.oid = proc.pronamespace
    WHERE function_namespace.nspname = 'climate'
      AND proc.proname = 'weather_metrics_reject_immutable'
)
SELECT md5(COALESCE(string_agg(part, E'\n' ORDER BY part), 'empty')) AS fingerprint
FROM parts;

DO $stage8$
DECLARE
    v_table_count integer;
    v_bad_relation boolean;
    v_orphan_object boolean;
    v_database_owner text;
    v_schema_owner text;
    v_marker text;
    v_fingerprint text;
BEGIN
    SELECT pg_get_userbyid(datdba)
    INTO v_database_owner
    FROM pg_database
    WHERE datname = current_database();
    IF v_database_owner IS DISTINCT FROM current_setting('stage8.admin_role', true) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'business database owner is incompatible';
    END IF;

    SELECT pg_get_userbyid(nspowner)
    INTO v_schema_owner
    FROM pg_namespace
    WHERE nspname = 'climate';
    IF v_schema_owner IS DISTINCT FROM current_setting('stage8.admin_role', true) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'business schema owner is incompatible';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'climate'
          AND relation.relname = ANY (
              ARRAY['locations', 'weather_metrics', 'weather_alerts']::name[]
          )
          AND (relation.relrowsecurity OR relation.relforcerowsecurity)
    ) OR EXISTS (
        SELECT 1
        FROM pg_policy AS row_policy
        JOIN pg_class AS relation ON relation.oid = row_policy.polrelid
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'climate'
          AND relation.relname = ANY (
              ARRAY['locations', 'weather_metrics', 'weather_alerts']::name[]
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'row-level security is incompatible with the business sinks';
    END IF;

    SELECT count(*)
    INTO v_table_count
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'climate'
      AND relation.relname = ANY (
          ARRAY['locations', 'weather_metrics', 'weather_alerts']::name[]
      );

    IF v_table_count = 0 THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'climate'
              AND relation.relname = ANY (
                  ARRAY[
                      'weather_metrics_location_end_idx',
                      'weather_alerts_location_end_idx'
                  ]::name[]
              )
        ) OR EXISTS (
            SELECT 1
            FROM pg_proc AS proc
            JOIN pg_namespace AS namespace ON namespace.oid = proc.pronamespace
            WHERE namespace.nspname = 'climate'
              AND proc.proname = 'weather_metrics_reject_immutable'
        )
        INTO v_orphan_object;
        IF v_orphan_object THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'orphaned business schema objects require manual review';
        END IF;
        RETURN;
    END IF;

    IF v_table_count <> 3 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'incomplete business schema requires manual review';
    END IF;

    SELECT bool_or(relation.relkind <> 'r')
    INTO v_bad_relation
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'climate'
      AND relation.relname = ANY (
          ARRAY['locations', 'weather_metrics', 'weather_alerts']::name[]
      );
    IF v_bad_relation THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'business schema object types are incompatible';
    END IF;

    SELECT obj_description(relation.oid, 'pg_class')
    INTO v_marker
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'climate'
      AND relation.relname = 'weather_metrics';
    SELECT fingerprint INTO v_fingerprint
    FROM pg_temp.stage8_business_schema_fingerprint;

    IF v_marker IS DISTINCT FROM 'stage8-business-schema-v1:' || v_fingerprint THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'business schema fingerprint is missing or incompatible';
    END IF;
END
$stage8$;

CREATE TABLE IF NOT EXISTS climate.locations (
    location_id text NOT NULL,
    city text NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    CONSTRAINT locations_pkey PRIMARY KEY (location_id),
    CONSTRAINT locations_location_id_nonempty_check CHECK (btrim(location_id) <> ''),
    CONSTRAINT locations_city_nonempty_check CHECK (btrim(city) <> ''),
    CONSTRAINT locations_latitude_check CHECK (
        latitude >= -90
        AND latitude <= 90
        AND latitude > '-Infinity'::double precision
        AND latitude < 'Infinity'::double precision
    ),
    CONSTRAINT locations_longitude_check CHECK (
        longitude >= -180
        AND longitude <= 180
        AND longitude > '-Infinity'::double precision
        AND longitude < 'Infinity'::double precision
    )
);

CREATE TABLE IF NOT EXISTS climate.weather_metrics (
    location_id text NOT NULL,
    window_start_ms bigint NOT NULL,
    window_end_ms bigint NOT NULL,
    event_count bigint NOT NULL,
    temperature_avg double precision NOT NULL,
    precipitation_max double precision NOT NULL,
    precipitation_rate_max_mm_per_hour double precision NOT NULL,
    wind_speed_max double precision NOT NULL,
    wind_gust_max double precision NOT NULL,
    visibility_min double precision NOT NULL,
    measurement_interval_min_seconds integer NOT NULL,
    measurement_interval_max_seconds integer NOT NULL,
    first_event_timestamp_ms bigint NOT NULL,
    last_event_timestamp_ms bigint NOT NULL,
    schema_version smallint NOT NULL,
    risk_score smallint NOT NULL,
    risk_level text NOT NULL,
    risk_rules_version text NOT NULL,
    risk_components jsonb NOT NULL,
    CONSTRAINT weather_metrics_pkey PRIMARY KEY (
        location_id,
        window_start_ms,
        window_end_ms,
        risk_rules_version
    ),
    CONSTRAINT weather_metrics_location_fk FOREIGN KEY (location_id)
        REFERENCES climate.locations (location_id),
    CONSTRAINT weather_metrics_window_check CHECK (window_end_ms > window_start_ms),
    CONSTRAINT weather_metrics_event_count_check CHECK (event_count > 0),
    CONSTRAINT weather_metrics_event_extremes_check CHECK (
        window_start_ms <= first_event_timestamp_ms
        AND first_event_timestamp_ms <= last_event_timestamp_ms
        AND last_event_timestamp_ms < window_end_ms
    ),
    CONSTRAINT weather_metrics_intervals_check CHECK (
        measurement_interval_min_seconds BETWEEN 1 AND 3600
        AND measurement_interval_max_seconds BETWEEN 1 AND 3600
        AND measurement_interval_min_seconds <= measurement_interval_max_seconds
    ),
    CONSTRAINT weather_metrics_schema_version_check CHECK (schema_version = 1),
    CONSTRAINT weather_metrics_rules_version_check CHECK (btrim(risk_rules_version) <> ''),
    CONSTRAINT weather_metrics_temperature_avg_check CHECK (
        temperature_avg BETWEEN -100 AND 100
        AND temperature_avg > '-Infinity'::double precision
        AND temperature_avg < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_precipitation_max_check CHECK (
        precipitation_max BETWEEN 0 AND 1000
        AND precipitation_max > '-Infinity'::double precision
        AND precipitation_max < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_precipitation_rate_check CHECK (
        precipitation_rate_max_mm_per_hour BETWEEN 0 AND 3600000
        AND precipitation_rate_max_mm_per_hour > '-Infinity'::double precision
        AND precipitation_rate_max_mm_per_hour < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_wind_speed_check CHECK (
        wind_speed_max BETWEEN 0 AND 500
        AND wind_speed_max > '-Infinity'::double precision
        AND wind_speed_max < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_wind_gust_check CHECK (
        wind_gust_max BETWEEN 0 AND 500
        AND wind_gust_max > '-Infinity'::double precision
        AND wind_gust_max < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_visibility_check CHECK (
        visibility_min BETWEEN 0 AND 1000000
        AND visibility_min > '-Infinity'::double precision
        AND visibility_min < 'Infinity'::double precision
    ),
    CONSTRAINT weather_metrics_risk_score_check CHECK (
        risk_score IN (0, 25, 50, 75, 100)
    ),
    CONSTRAINT weather_metrics_risk_level_check CHECK (
        risk_level = CASE
            WHEN risk_score < 25 THEN 'LOW'
            WHEN risk_score < 50 THEN 'MEDIUM'
            WHEN risk_score < 75 THEN 'HIGH'
            ELSE 'CRITICAL'
        END
    ),
    CONSTRAINT weather_metrics_risk_components_shape_check CHECK (
        CASE WHEN jsonb_typeof(risk_components) = 'object' THEN
            risk_components ?& ARRAY[
                'precipitation_rate', 'wind_speed', 'wind_gust', 'visibility'
            ]::text[]
            AND (
                risk_components - ARRAY[
                    'precipitation_rate', 'wind_speed', 'wind_gust', 'visibility'
                ]::text[]
            ) = '{}'::jsonb
        ELSE false
        END
    ),
    CONSTRAINT weather_metrics_risk_component_values_check CHECK (
        jsonb_typeof((risk_components->'precipitation_rate')) = 'number'
        AND jsonb_typeof((risk_components->'wind_speed')) = 'number'
        AND jsonb_typeof((risk_components->'wind_gust')) = 'number'
        AND jsonb_typeof((risk_components->'visibility')) = 'number'
        AND
        COALESCE((risk_components->>'precipitation_rate') ~ '^(0|25|50|75|100)$', false)
        AND COALESCE((risk_components->>'wind_speed') ~ '^(0|25|50|75|100)$', false)
        AND COALESCE((risk_components->>'wind_gust') ~ '^(0|25|50|75|100)$', false)
        AND COALESCE((risk_components->>'visibility') ~ '^(0|25|50|75|100)$', false)
    ),
    CONSTRAINT weather_metrics_risk_components_match_check CHECK (
        CASE
            WHEN COALESCE(
                (risk_components->>'precipitation_rate') ~ '^(0|25|50|75|100)$', false
            )
             AND COALESCE((risk_components->>'wind_speed') ~ '^(0|25|50|75|100)$', false)
             AND COALESCE((risk_components->>'wind_gust') ~ '^(0|25|50|75|100)$', false)
             AND COALESCE((risk_components->>'visibility') ~ '^(0|25|50|75|100)$', false)
            THEN
                (risk_components->>'precipitation_rate') = (
                    CASE
                        WHEN precipitation_rate_max_mm_per_hour >= 60 THEN 100
                        WHEN precipitation_rate_max_mm_per_hour >= 30 THEN 75
                        WHEN precipitation_rate_max_mm_per_hour >= 20 THEN 50
                        WHEN precipitation_rate_max_mm_per_hour >= 2 THEN 25
                        ELSE 0
                    END
                )::text
                AND (risk_components->>'wind_speed') = (
                    CASE
                        WHEN wind_speed_max >= 100 THEN 100
                        WHEN wind_speed_max >= 60 THEN 75
                        WHEN wind_speed_max >= 40 THEN 50
                        WHEN wind_speed_max >= 20 THEN 25
                        ELSE 0
                    END
                )::text
                AND (risk_components->>'wind_gust') = (
                    CASE
                        WHEN wind_gust_max >= 120 THEN 100
                        WHEN wind_gust_max >= 80 THEN 75
                        WHEN wind_gust_max >= 50 THEN 50
                        WHEN wind_gust_max >= 30 THEN 25
                        ELSE 0
                    END
                )::text
                AND (risk_components->>'visibility') = (
                    CASE
                        WHEN visibility_min < 200 THEN 100
                        WHEN visibility_min < 1000 THEN 75
                        WHEN visibility_min < 2000 THEN 50
                        WHEN visibility_min < 5000 THEN 25
                        ELSE 0
                    END
                )::text
                AND risk_score = GREATEST(
                    CASE
                        WHEN precipitation_rate_max_mm_per_hour >= 60 THEN 100
                        WHEN precipitation_rate_max_mm_per_hour >= 30 THEN 75
                        WHEN precipitation_rate_max_mm_per_hour >= 20 THEN 50
                        WHEN precipitation_rate_max_mm_per_hour >= 2 THEN 25
                        ELSE 0
                    END,
                    CASE
                        WHEN wind_speed_max >= 100 THEN 100
                        WHEN wind_speed_max >= 60 THEN 75
                        WHEN wind_speed_max >= 40 THEN 50
                        WHEN wind_speed_max >= 20 THEN 25
                        ELSE 0
                    END,
                    CASE
                        WHEN wind_gust_max >= 120 THEN 100
                        WHEN wind_gust_max >= 80 THEN 75
                        WHEN wind_gust_max >= 50 THEN 50
                        WHEN wind_gust_max >= 30 THEN 25
                        ELSE 0
                    END,
                    CASE
                        WHEN visibility_min < 200 THEN 100
                        WHEN visibility_min < 1000 THEN 75
                        WHEN visibility_min < 2000 THEN 50
                        WHEN visibility_min < 5000 THEN 25
                        ELSE 0
                    END
                )
            ELSE false
        END
    )
);

CREATE TABLE IF NOT EXISTS climate.weather_alerts (
    alert_id text NOT NULL,
    location_id text NOT NULL,
    window_start_ms bigint NOT NULL,
    window_end_ms bigint NOT NULL,
    risk_rules_version text NOT NULL,
    risk_score smallint NOT NULL,
    risk_level text NOT NULL,
    payload jsonb NOT NULL,
    CONSTRAINT weather_alerts_pkey PRIMARY KEY (alert_id),
    CONSTRAINT weather_alerts_identity_key UNIQUE (
        location_id,
        window_start_ms,
        window_end_ms,
        risk_rules_version
    ),
    CONSTRAINT weather_alerts_metric_fk FOREIGN KEY (
        location_id,
        window_start_ms,
        window_end_ms,
        risk_rules_version
    ) REFERENCES climate.weather_metrics (
        location_id,
        window_start_ms,
        window_end_ms,
        risk_rules_version
    ),
    CONSTRAINT weather_alerts_alert_id_check CHECK (alert_id ~ '^[0-9a-f]{64}$'),
    CONSTRAINT weather_alerts_window_check CHECK (window_end_ms > window_start_ms),
    CONSTRAINT weather_alerts_risk_score_check CHECK (risk_score IN (50, 75, 100)),
    CONSTRAINT weather_alerts_risk_level_check CHECK (
        risk_level = CASE WHEN risk_score < 75 THEN 'HIGH' ELSE 'CRITICAL' END
    ),
    CONSTRAINT weather_alerts_payload_check CHECK (
        (
            CASE WHEN jsonb_typeof(payload) = 'object' THEN
                payload ?& ARRAY[
                    'component', 'operation', 'schema_version', 'location_id',
                    'window_start_ms', 'window_end_ms', 'event_count', 'temperature_avg',
                    'precipitation_max', 'precipitation_rate_max_mm_per_hour',
                    'wind_speed_max', 'wind_gust_max', 'visibility_min',
                    'measurement_interval_min_seconds', 'measurement_interval_max_seconds',
                    'first_event_timestamp_ms', 'last_event_timestamp_ms', 'risk_score',
                    'risk_level', 'risk_components', 'risk_rules_version', 'alert_id'
                ]::text[]
                AND (
                    payload - ARRAY[
                        'component', 'operation', 'schema_version', 'location_id',
                        'window_start_ms', 'window_end_ms', 'event_count', 'temperature_avg',
                        'precipitation_max', 'precipitation_rate_max_mm_per_hour',
                        'wind_speed_max', 'wind_gust_max', 'visibility_min',
                        'measurement_interval_min_seconds', 'measurement_interval_max_seconds',
                        'first_event_timestamp_ms', 'last_event_timestamp_ms', 'risk_score',
                        'risk_level', 'risk_components', 'risk_rules_version', 'alert_id'
                    ]::text[]
                ) = '{}'::jsonb
                AND (payload->>'component') = 'flink'
                AND (payload->>'operation') = 'weather_alert'
                AND (payload->>'schema_version') = '1'
                AND (payload->>'alert_id') = alert_id
                AND (payload->>'location_id') = location_id
                AND (payload->>'window_start_ms') = window_start_ms::text
                AND (payload->>'window_end_ms') = window_end_ms::text
                AND (payload->>'risk_score') = risk_score::text
                AND (payload->>'risk_level') = risk_level
                AND (payload->>'risk_rules_version') = risk_rules_version
                AND jsonb_typeof((payload->'schema_version')) = 'number'
                AND jsonb_typeof((payload->'window_start_ms')) = 'number'
                AND jsonb_typeof((payload->'window_end_ms')) = 'number'
                AND jsonb_typeof((payload->'event_count')) = 'number'
                AND jsonb_typeof((payload->'temperature_avg')) = 'number'
                AND jsonb_typeof((payload->'precipitation_max')) = 'number'
                AND jsonb_typeof((payload->'precipitation_rate_max_mm_per_hour')) = 'number'
                AND jsonb_typeof((payload->'wind_speed_max')) = 'number'
                AND jsonb_typeof((payload->'wind_gust_max')) = 'number'
                AND jsonb_typeof((payload->'visibility_min')) = 'number'
                AND jsonb_typeof((payload->'measurement_interval_min_seconds')) = 'number'
                AND jsonb_typeof((payload->'measurement_interval_max_seconds')) = 'number'
                AND jsonb_typeof((payload->'first_event_timestamp_ms')) = 'number'
                AND jsonb_typeof((payload->'last_event_timestamp_ms')) = 'number'
                AND jsonb_typeof((payload->'risk_score')) = 'number'
                AND (payload->>'event_count') ~ '^[1-9][0-9]*$'
                AND (payload->'event_count') <= '9223372036854775807'::jsonb
                AND (payload->'temperature_avg') >= '-100'::jsonb
                AND (payload->'temperature_avg') <= '100'::jsonb
                AND (payload->'precipitation_max') >= '0'::jsonb
                AND (payload->'precipitation_max') <= '1000'::jsonb
                AND (payload->'precipitation_rate_max_mm_per_hour') >= '0'::jsonb
                AND (payload->'precipitation_rate_max_mm_per_hour') <= '3600000'::jsonb
                AND (payload->'wind_speed_max') >= '0'::jsonb
                AND (payload->'wind_speed_max') <= '500'::jsonb
                AND (payload->'wind_gust_max') >= '0'::jsonb
                AND (payload->'wind_gust_max') <= '500'::jsonb
                AND (payload->'visibility_min') >= '0'::jsonb
                AND (payload->'visibility_min') <= '1000000'::jsonb
                AND (payload->>'measurement_interval_min_seconds')
                    ~ '^[1-9][0-9]*$'
                AND (payload->>'measurement_interval_max_seconds')
                    ~ '^[1-9][0-9]*$'
                AND (payload->'measurement_interval_min_seconds') >= '1'::jsonb
                AND (payload->'measurement_interval_min_seconds') <= '3600'::jsonb
                AND (payload->'measurement_interval_max_seconds') >= '1'::jsonb
                AND (payload->'measurement_interval_max_seconds') <= '3600'::jsonb
                AND (payload->'measurement_interval_min_seconds')
                    <= (payload->'measurement_interval_max_seconds')
                AND (payload->>'first_event_timestamp_ms')
                    ~ '^-?(0|[1-9][0-9]*)$'
                AND (payload->>'last_event_timestamp_ms')
                    ~ '^-?(0|[1-9][0-9]*)$'
                AND (payload->'first_event_timestamp_ms')
                    >= '-9223372036854775808'::jsonb
                AND (payload->'first_event_timestamp_ms')
                    <= '9223372036854775807'::jsonb
                AND (payload->'last_event_timestamp_ms')
                    >= '-9223372036854775808'::jsonb
                AND (payload->'last_event_timestamp_ms')
                    <= '9223372036854775807'::jsonb
                AND (payload->'window_start_ms')
                    <= (payload->'first_event_timestamp_ms')
                AND (payload->'first_event_timestamp_ms')
                    <= (payload->'last_event_timestamp_ms')
                AND (payload->'last_event_timestamp_ms')
                    < (payload->'window_end_ms')
                AND CASE WHEN jsonb_typeof((payload->'risk_components')) = 'object' THEN
                    (payload->'risk_components') ?& ARRAY[
                        'precipitation_rate', 'wind_speed', 'wind_gust', 'visibility'
                    ]::text[]
                    AND (
                        (payload->'risk_components') - ARRAY[
                            'precipitation_rate', 'wind_speed', 'wind_gust', 'visibility'
                        ]::text[]
                    ) = '{}'::jsonb
                    AND jsonb_typeof(
                        (payload->'risk_components')->'precipitation_rate'
                    ) = 'number'
                    AND jsonb_typeof((payload->'risk_components')->'wind_speed') = 'number'
                    AND jsonb_typeof((payload->'risk_components')->'wind_gust') = 'number'
                    AND jsonb_typeof((payload->'risk_components')->'visibility') = 'number'
                    AND COALESCE(
                        (payload->'risk_components'->>'precipitation_rate')
                            ~ '^(0|25|50|75|100)$', false
                    )
                    AND COALESCE(
                        (payload->'risk_components'->>'wind_speed')
                            ~ '^(0|25|50|75|100)$', false
                    )
                    AND COALESCE(
                        (payload->'risk_components'->>'wind_gust')
                            ~ '^(0|25|50|75|100)$', false
                    )
                    AND COALESCE(
                        (payload->'risk_components'->>'visibility')
                            ~ '^(0|25|50|75|100)$', false
                    )
                    AND CASE
                        WHEN COALESCE(
                            (payload->'risk_components'->>'precipitation_rate')
                                ~ '^(0|25|50|75|100)$', false
                        )
                         AND COALESCE(
                            (payload->'risk_components'->>'wind_speed')
                                ~ '^(0|25|50|75|100)$', false
                        )
                         AND COALESCE(
                            (payload->'risk_components'->>'wind_gust')
                                ~ '^(0|25|50|75|100)$', false
                        )
                         AND COALESCE(
                            (payload->'risk_components'->>'visibility')
                                ~ '^(0|25|50|75|100)$', false
                        )
                        THEN
                            (payload->'risk_components'->>'precipitation_rate') = (
                                CASE
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '60'::jsonb THEN 100
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '30'::jsonb THEN 75
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '20'::jsonb THEN 50
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '2'::jsonb THEN 25
                                    ELSE 0
                                END
                            )::text
                            AND (payload->'risk_components'->>'wind_speed') = (
                                CASE
                                    WHEN (payload->'wind_speed_max') >= '100'::jsonb THEN 100
                                    WHEN (payload->'wind_speed_max') >= '60'::jsonb THEN 75
                                    WHEN (payload->'wind_speed_max') >= '40'::jsonb THEN 50
                                    WHEN (payload->'wind_speed_max') >= '20'::jsonb THEN 25
                                    ELSE 0
                                END
                            )::text
                            AND (payload->'risk_components'->>'wind_gust') = (
                                CASE
                                    WHEN (payload->'wind_gust_max') >= '120'::jsonb THEN 100
                                    WHEN (payload->'wind_gust_max') >= '80'::jsonb THEN 75
                                    WHEN (payload->'wind_gust_max') >= '50'::jsonb THEN 50
                                    WHEN (payload->'wind_gust_max') >= '30'::jsonb THEN 25
                                    ELSE 0
                                END
                            )::text
                            AND (payload->'risk_components'->>'visibility') = (
                                CASE
                                    WHEN (payload->'visibility_min') < '200'::jsonb THEN 100
                                    WHEN (payload->'visibility_min') < '1000'::jsonb THEN 75
                                    WHEN (payload->'visibility_min') < '2000'::jsonb THEN 50
                                    WHEN (payload->'visibility_min') < '5000'::jsonb THEN 25
                                    ELSE 0
                                END
                            )::text
                            AND risk_score = GREATEST(
                                CASE
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '60'::jsonb THEN 100
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '30'::jsonb THEN 75
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '20'::jsonb THEN 50
                                    WHEN (payload->'precipitation_rate_max_mm_per_hour')
                                        >= '2'::jsonb THEN 25
                                    ELSE 0
                                END,
                                CASE
                                    WHEN (payload->'wind_speed_max') >= '100'::jsonb THEN 100
                                    WHEN (payload->'wind_speed_max') >= '60'::jsonb THEN 75
                                    WHEN (payload->'wind_speed_max') >= '40'::jsonb THEN 50
                                    WHEN (payload->'wind_speed_max') >= '20'::jsonb THEN 25
                                    ELSE 0
                                END,
                                CASE
                                    WHEN (payload->'wind_gust_max') >= '120'::jsonb THEN 100
                                    WHEN (payload->'wind_gust_max') >= '80'::jsonb THEN 75
                                    WHEN (payload->'wind_gust_max') >= '50'::jsonb THEN 50
                                    WHEN (payload->'wind_gust_max') >= '30'::jsonb THEN 25
                                    ELSE 0
                                END,
                                CASE
                                    WHEN (payload->'visibility_min') < '200'::jsonb THEN 100
                                    WHEN (payload->'visibility_min') < '1000'::jsonb THEN 75
                                    WHEN (payload->'visibility_min') < '2000'::jsonb THEN 50
                                    WHEN (payload->'visibility_min') < '5000'::jsonb THEN 25
                                    ELSE 0
                                END
                            )
                        ELSE false
                    END
                ELSE false END
            ELSE false END
        ) IS TRUE
    )
);

CREATE INDEX IF NOT EXISTS weather_metrics_location_end_idx
    ON climate.weather_metrics (location_id, window_end_ms DESC);
CREATE INDEX IF NOT EXISTS weather_alerts_location_end_idx
    ON climate.weather_alerts (location_id, window_end_ms DESC);

CREATE OR REPLACE FUNCTION climate.weather_metrics_reject_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'weather_metrics identity is immutable';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION climate.weather_metrics_reject_immutable() FROM PUBLIC;

DO $stage8$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'climate.weather_metrics'::regclass
          AND tgname = 'weather_metrics_immutable_identity_trigger'
          AND NOT tgisinternal
    ) THEN
        EXECUTE
            'CREATE TRIGGER weather_metrics_immutable_identity_trigger '
            || 'BEFORE UPDATE ON climate.weather_metrics '
            || 'FOR EACH ROW EXECUTE FUNCTION climate.weather_metrics_reject_immutable()';
    END IF;
END
$stage8$;

CREATE TEMP TABLE stage8_location_seed (
    location_id text NOT NULL PRIMARY KEY,
    city text NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL
) ON COMMIT DROP;

\copy pg_temp.stage8_location_seed (location_id, city, latitude, longitude) FROM '/opt/climate/postgres/locations.csv' WITH (FORMAT csv, HEADER true)

DO $stage8$
DECLARE
    v_marker text;
BEGIN
    SELECT obj_description('climate.weather_metrics'::regclass, 'pg_class')
    INTO v_marker;
    IF v_marker IS NOT NULL AND (
        EXISTS (
            SELECT location_id, city, latitude, longitude
            FROM pg_temp.stage8_location_seed
            EXCEPT
            SELECT location_id, city, latitude, longitude
            FROM climate.locations
        ) OR EXISTS (
            SELECT location_id, city, latitude, longitude
            FROM climate.locations
            EXCEPT
            SELECT location_id, city, latitude, longitude
            FROM pg_temp.stage8_location_seed
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'location catalog drift requires manual review';
    END IF;
END
$stage8$;

INSERT INTO climate.locations (location_id, city, latitude, longitude)
SELECT location_id, city, latitude, longitude
FROM pg_temp.stage8_location_seed
ON CONFLICT (location_id) DO NOTHING;

DO $stage8$
BEGIN
    IF EXISTS (
        SELECT location_id, city, latitude, longitude
        FROM pg_temp.stage8_location_seed
        EXCEPT
        SELECT location_id, city, latitude, longitude
        FROM climate.locations
    ) OR EXISTS (
        SELECT location_id, city, latitude, longitude
        FROM climate.locations
        EXCEPT
        SELECT location_id, city, latitude, longitude
        FROM pg_temp.stage8_location_seed
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'location catalog does not match the generated seed';
    END IF;
END
$stage8$;

REVOKE ALL ON SCHEMA climate FROM PUBLIC;
REVOKE CREATE ON SCHEMA climate FROM :"migration_app";
GRANT USAGE ON SCHEMA climate TO :"migration_app";
GRANT CONNECT ON DATABASE :"migration_database" TO :"migration_app";
REVOKE TEMPORARY ON DATABASE :"migration_database" FROM PUBLIC;
REVOKE TEMPORARY ON DATABASE :"migration_database" FROM :"migration_app";
GRANT TEMPORARY ON DATABASE :"migration_database" TO :"migration_admin";

REVOKE ALL ON TABLE climate.locations, climate.weather_metrics, climate.weather_alerts
    FROM PUBLIC;
REVOKE ALL ON TABLE climate.locations, climate.weather_metrics, climate.weather_alerts
    FROM :"migration_app";
GRANT SELECT ON TABLE climate.locations TO :"migration_app";
GRANT SELECT, INSERT, UPDATE
    ON TABLE climate.weather_metrics, climate.weather_alerts
    TO :"migration_app";

DO $stage8$
DECLARE
    v_expected text;
    v_marker text;
BEGIN
    SELECT 'stage8-business-schema-v1:' || fingerprint
    INTO v_expected
    FROM pg_temp.stage8_business_schema_fingerprint;
    SELECT obj_description('climate.weather_metrics'::regclass, 'pg_class')
    INTO v_marker;

    IF v_marker IS NULL THEN
        EXECUTE format(
            'COMMENT ON TABLE climate.weather_metrics IS %L',
            v_expected
        );
    ELSIF v_marker IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'business schema changed during migration';
    END IF;
END
$stage8$;

COMMIT;
