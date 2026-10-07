#!/usr/bin/env bash
set -Eeuo pipefail
export PGPASSWORD="$POSTGRES_PASSWORD"
case "${1:-}" in
  before)
    psql -X -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
CREATE SCHEMA foundation_verification;
CREATE TABLE foundation_verification.marker (id integer PRIMARY KEY);
INSERT INTO foundation_verification.marker VALUES (1);
COMMIT;
SQL
    ;;
  after)
    value=$(psql -X -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c 'SELECT id FROM foundation_verification.marker')
    test "$value" = 1
    psql -X -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
DROP TABLE foundation_verification.marker;
DROP SCHEMA foundation_verification;
COMMIT;
SQL
    printf 'Persistence after restart: PASS; test marker removed\n'
    ;;
  *) exit 2 ;;
esac
