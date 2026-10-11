from __future__ import annotations

from typing import Any

BusinessKey = tuple[str, int, int, str]


def _identity(row: dict[str, Any]) -> BusinessKey:
    return (
        str(row["location_id"]),
        int(row["window_start_ms"]),
        int(row["window_end_ms"]),
        str(row["risk_rules_version"]),
    )


def _semantic_equal(actual: object, expected: object) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(expected, int):
        return type(actual) is int and actual == expected
    if isinstance(expected, float):
        return isinstance(actual, int | float) and float(actual) == expected
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            return False
        return all(_semantic_equal(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(_semantic_equal(left, right) for left, right in zip(actual, expected))
        )
    return type(actual) is type(expected) and actual == expected


def legacy_snapshot_additions(
    before: dict[str, list[dict[str, Any]]],
    after: dict[str, list[dict[str, Any]]],
) -> tuple[set[BusinessKey], set[BusinessKey]]:
    """Return only new metric and alert identities from two full snapshots."""
    added_by_table: dict[str, set[BusinessKey]] = {}
    for table in ("metrics", "alerts"):
        before_by_key = {_identity(row): row for row in before[table]}
        after_by_key = {_identity(row): row for row in after[table]}
        if len(before_by_key) != len(before[table]) or len(after_by_key) != len(after[table]):
            raise AssertionError(f"duplicate business identity in {table} snapshot")
        added_by_table[table] = after_by_key.keys() - before_by_key.keys()
    return added_by_table["metrics"], added_by_table["alerts"]


def legacy_snapshot_delta(
    before: dict[str, list[dict[str, Any]]],
    after: dict[str, list[dict[str, Any]]],
) -> tuple[set[BusinessKey], set[BusinessKey]]:
    """Prove existing business rows stayed fixed and return exact additions only."""
    added_metrics, added_alerts = legacy_snapshot_additions(before, after)
    for table in ("metrics", "alerts"):
        before_rows = before[table]
        after_rows = after[table]
        before_by_key = {_identity(row): row for row in before_rows}
        after_by_key = {_identity(row): row for row in after_rows}
        missing = before_by_key.keys() - after_by_key.keys()
        if missing:
            raise AssertionError(
                f"pre-existing business row disappeared from {table}: {sorted(missing)}"
            )
        for key, original in before_by_key.items():
            if not _semantic_equal(after_by_key[key], original):
                raise AssertionError(f"pre-existing business row changed in {table}: {key}")
    return added_metrics, added_alerts
