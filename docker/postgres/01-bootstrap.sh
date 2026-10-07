#!/usr/bin/env bash
set -Eeuo pipefail

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${APP_DB_USER:?APP_DB_USER is required}"
: "${APP_DB_PASSWORD:?APP_DB_PASSWORD is required}"

psql --no-psqlrc --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 <<'PSQL'
\getenv app_db_user APP_DB_USER
\getenv app_db_password APP_DB_PASSWORD
\getenv postgres_user POSTGRES_USER
\getenv postgres_db POSTGRES_DB

BEGIN;
SELECT format(
  'CREATE ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
  :'app_db_user',
  :'app_db_password'
) \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA climate AUTHORIZATION :"postgres_user";
GRANT CONNECT ON DATABASE :"postgres_db" TO :"app_db_user";
GRANT USAGE ON SCHEMA climate TO :"app_db_user";
COMMIT;
PSQL
