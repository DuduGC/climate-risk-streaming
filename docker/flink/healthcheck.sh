#!/bin/sh
set -eu

role="${1:-}"

case "$role" in
  jobmanager)
    curl --fail --silent --show-error --max-time 3 \
      http://127.0.0.1:8081/overview >/dev/null
    ;;
  taskmanager)
    rest_url="${FLINK_JOBMANAGER_REST_URL:-http://flink-jobmanager:8081}"
    taskmanager_hostname="$(hostname)"
    taskmanagers="$(curl --fail --silent --show-error --max-time 3 \
      "${rest_url%/}/taskmanagers")"

    # A registered TaskManager exposes its RPC path in this REST response.
    # The configured Flink host and the container hostname are both
    # flink-taskmanager, so this rejects an empty or unrelated registration.
    printf '%s\n' "$taskmanagers" \
      | grep -Eo '"path"[[:space:]]*:[[:space:]]*"[^"]*"' \
      | grep -F "$taskmanager_hostname" >/dev/null
    ;;
  *)
    echo "usage: $0 {jobmanager|taskmanager}" >&2
    exit 2
    ;;
esac
