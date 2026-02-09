"""Upgrade older sqlite-history-json databases to the current schema.

Run as::

    python -m sqlite_history_json.upgrade database.db
    python -m sqlite_history_json.upgrade database.db --dry-run

The upgrade detects audit tables created before change-grouping support
was added and:

1. Creates the ``_history_json`` groups table (if it does not exist).
2. Adds a ``[group]`` column to each audit table that is missing it.
3. Drops and recreates triggers so they populate the new column.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys

from .core import (
    _audit_table_name,
    _build_delete_trigger_sql,
    _build_insert_trigger_sql,
    _build_update_trigger_sql,
    _ensure_groups_table,
    _get_non_pk_columns,
    _get_pk_columns,
    _get_table_info,
    _GROUPS_TABLE,
)


def _find_audit_tables(conn: sqlite3.Connection) -> list[str]:
    """Return names of all audit tables (``_history_json_*``)."""
    prefix = "_history_json_"
    rows = conn.execute(
        "select name from sqlite_master where type = 'table' "
        "and name like ? escape '\\'",
        (prefix.replace("_", "\\_", 2) + "%",),
    ).fetchall()
    return [r[0] for r in rows]


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check whether *table* already has a column named *column*."""
    cols = conn.execute(f"pragma table_info([{table}])").fetchall()
    return any(r[1] == column for r in cols)


def _source_table_for(audit_name: str) -> str:
    """Derive the source table name from an audit table name."""
    return audit_name[len("_history_json_"):]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "select count(*) from sqlite_master where type = 'table' and name = ?",
            (name,),
        ).fetchone()[0]
        > 0
    )


def _trigger_sql(conn: sqlite3.Connection, trigger_name: str) -> str | None:
    """Return the SQL body of an existing trigger, or None."""
    row = conn.execute(
        "select sql from sqlite_master where type = 'trigger' and name = ?",
        (trigger_name,),
    ).fetchone()
    return row[0] if row else None


def _trigger_needs_upgrade(conn: sqlite3.Connection, audit_name: str) -> bool:
    """Return True if any trigger for *audit_name* is missing the group subquery."""
    for suffix in ("_insert", "_update", "_delete"):
        sql = _trigger_sql(conn, f"{audit_name}{suffix}")
        if sql is not None and "[group]" not in sql:
            return True
    return False


def detect_upgrades(conn: sqlite3.Connection) -> list[dict]:
    """Scan the database and return a list of upgrade actions needed.

    Each item is a dict with keys:

    * ``audit_table`` – the audit table name
    * ``source_table`` – the tracked source table name (or ``None``)
    * ``needs_column`` – ``True`` if the ``[group]`` column is missing
    * ``needs_triggers`` – ``True`` if triggers need to be recreated
    * ``source_exists`` – ``True`` if the source table still exists
    """
    groups_table_exists = _table_exists(conn, _GROUPS_TABLE)

    actions = []
    for audit_name in _find_audit_tables(conn):
        source_table = _source_table_for(audit_name)
        source_exists = _table_exists(conn, source_table)
        needs_column = not _has_column(conn, audit_name, "group")
        needs_triggers = (
            source_exists and _trigger_needs_upgrade(conn, audit_name)
        )

        if needs_column or needs_triggers:
            actions.append(
                {
                    "audit_table": audit_name,
                    "source_table": source_table,
                    "needs_column": needs_column,
                    "needs_triggers": needs_triggers,
                    "source_exists": source_exists,
                }
            )

    return actions


def apply_upgrade(conn: sqlite3.Connection) -> list[dict]:
    """Apply all detected upgrades and return the list of actions taken.

    Returns the same structure as :func:`detect_upgrades` for the items
    that were actually upgraded.
    """
    actions = detect_upgrades(conn)
    if not actions:
        return []

    # Ensure the groups table exists first (column FK target)
    _ensure_groups_table(conn)

    for action in actions:
        audit_name = action["audit_table"]
        source_table = action["source_table"]

        # 1. Add the [group] column if missing
        if action["needs_column"]:
            conn.execute(
                f"alter table [{audit_name}] "
                f"add column [group] integer references [{_GROUPS_TABLE}](id)"
            )

        # 2. Recreate triggers if the source table still exists
        if action["needs_triggers"] and action["source_exists"]:
            columns = _get_table_info(conn, source_table)
            pk_cols = _get_pk_columns(columns)
            non_pk_cols = _get_non_pk_columns(columns)

            # Drop old triggers
            for suffix in ("_insert", "_update", "_delete"):
                conn.execute(
                    f"drop trigger if exists [{audit_name}{suffix}]"
                )

            # Create new triggers (with [group] subquery)
            conn.execute(
                _build_insert_trigger_sql(
                    source_table, audit_name, pk_cols, non_pk_cols
                )
            )
            conn.execute(
                _build_update_trigger_sql(
                    source_table, audit_name, pk_cols, non_pk_cols
                )
            )
            conn.execute(
                _build_delete_trigger_sql(source_table, audit_name, pk_cols)
            )

    return actions


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m sqlite_history_json.upgrade",
        description=(
            "Upgrade an older sqlite-history-json database to the current schema."
        ),
    )
    parser.add_argument("database", help="Path to the SQLite database file.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be done without making changes.",
    )
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.database)
    try:
        if args.dry_run:
            actions = detect_upgrades(conn)
            if not actions:
                print("Nothing to upgrade.", file=sys.stderr)
            else:
                for action in actions:
                    parts = []
                    if action["needs_column"]:
                        parts.append("add [group] column")
                    if action["needs_triggers"]:
                        parts.append("recreate triggers")
                    print(
                        f"Would upgrade {action['audit_table']}: "
                        + ", ".join(parts),
                        file=sys.stderr,
                    )
        else:
            actions = apply_upgrade(conn)
            if not actions:
                print("Nothing to upgrade.", file=sys.stderr)
            else:
                conn.commit()
                for action in actions:
                    parts = []
                    if action["needs_column"]:
                        parts.append("added [group] column")
                    if action["needs_triggers"]:
                        parts.append("recreated triggers")
                    print(
                        f"Upgraded {action['audit_table']}: "
                        + ", ".join(parts),
                        file=sys.stderr,
                    )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
