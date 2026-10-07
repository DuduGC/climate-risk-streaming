#!/usr/bin/env bash
# Executado somente para validar a fundação; nunca imprime credenciais.
set -Eeuo pipefail
export PGPASSWORD="$APP_DB_PASSWORD"
result=$(psql -X -h 127.0.0.1 -U "$APP_DB_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 <<'SQL'
SELECT current_user;
SELECT (NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls)
FROM pg_roles WHERE rolname = current_user;
SELECT has_database_privilege(current_user, current_database(), 'CONNECT');
SELECT has_schema_privilege(current_user, 'climate', 'USAGE');
SELECT has_schema_privilege(current_user, 'climate', 'CREATE');
SELECT has_schema_privilege(current_user, 'public', 'CREATE');
SQL
)
expected=$(printf '%s\nt\nt\nt\nf\nf' "$APP_DB_USER")
if [ "$result" != "$expected" ]; then
  printf 'Unexpected privilege result:\n%s\n' "$result" >&2
  exit 1
fi
printf 'Authenticated app login and restricted privileges: PASS\n'
