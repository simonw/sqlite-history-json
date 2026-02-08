"""Core implementation of sqlite-history-json.

Implements the "updated values" JSON audit log pattern for SQLite tables,
using triggers to record INSERT, UPDATE, and DELETE operations.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from itertools import count


_savepoint_counter = count(1)


def _run_in_savepoint(conn: sqlite3.Connection, fn) -> None:
    """Execute fn() atomically using a SAVEPOINT."""
    savepoint_name = f"sqlite_history_json_sp_{next(_savepoint_counter)}"
    conn.execute(f"savepoint [{savepoint_name}]")
    try:
        fn()
    except Exception:
        conn.execute(f"rollback to [{savepoint_name}]")
        conn.execute(f"release [{savepoint_name}]")
        raise
    else:
        conn.execute(f"release [{savepoint_name}]")


_GROUPS_TABLE = "_history_json"


def _ensure_groups_table(conn: sqlite3.Connection) -> None:
    """Create the shared change-groups table if it does not already exist."""
    conn.execute(
        f"""create table if not exists [{_GROUPS_TABLE}] (
    id integer primary key,
    note text,
    current integer
)"""
    )
    conn.execute(
        f"create index if not exists [{_GROUPS_TABLE}_current] "
        f"on [{_GROUPS_TABLE}] (current)"
    )


@contextmanager
def change_group(conn: sqlite3.Connection, note: str | None = None):
    """Context manager that groups all audit entries created within it.

    Every audit-log row inserted by triggers while this context is active
    will share the same ``group`` id.  An optional *note* can be attached
    to describe the batch of changes.

    Yields the integer group id so callers can reference it later.

    Usage::

        with change_group(conn, note="bulk import") as group_id:
            conn.execute("INSERT INTO items ...")
            conn.execute("UPDATE items SET ...")
    """
    _ensure_groups_table(conn)
    # Clear any stale current marker (defensive, e.g. after a crash)
    conn.execute(
        f"update [{_GROUPS_TABLE}] set current = null where current = 1"
    )
    cursor = conn.execute(
        f"insert into [{_GROUPS_TABLE}] (note, current) values (?, 1)", [note]
    )
    group_id = cursor.lastrowid
    try:
        yield group_id
    finally:
        conn.execute(
            f"update [{_GROUPS_TABLE}] set current = null where current = 1"
        )


def _audit_table_name(table_name: str) -> str:
    return f"_history_json_{table_name}"


def _audit_pk_col_name(source_col_name: str) -> str:
    """Return the audit table column name for a source PK column."""
    return f"pk_{source_col_name}"


def _get_table_info(conn: sqlite3.Connection, table_name: str) -> list[dict]:
    """Return column info for a table via PRAGMA table_info."""
    rows = conn.execute(f"PRAGMA table_info([{table_name}])").fetchall()
    return [
        {"cid": r[0], "name": r[1], "type": r[2], "notnull": r[3], "pk": r[5]}
        for r in rows
    ]


def _get_pk_columns(columns: list[dict]) -> list[dict]:
    """Return the primary key columns, ordered by pk index."""
    pks = [c for c in columns if c["pk"] > 0]
    pks.sort(key=lambda c: c["pk"])
    return pks


def _get_non_pk_columns(columns: list[dict]) -> list[dict]:
    """Return the non-primary-key columns."""
    return [c for c in columns if c["pk"] == 0]


def _is_blob_type(col_type: str) -> bool:
    """Check if a column type is BLOB."""
    return col_type.upper() == "BLOB"


def _build_insert_trigger_sql(
    table_name: str,
    audit_name: str,
    pk_cols: list[dict],
    non_pk_cols: list[dict],
) -> str:
    """Build the AFTER INSERT trigger SQL."""
    audit_pk_col_names = ", ".join(
        f"[{_audit_pk_col_name(c['name'])}]" for c in pk_cols
    )
    pk_new_refs = ", ".join(f"NEW.[{c['name']}]" for c in pk_cols)

    # Build json_object arguments for all non-PK columns
    json_args = []
    for col in non_pk_cols:
        name = col["name"]
        if _is_blob_type(col["type"]):
            val_expr = (
                f"case when NEW.[{name}] is null "
                f"then json_object('null', 1) "
                f"else json_object('hex', hex(NEW.[{name}])) end"
            )
        else:
            val_expr = (
                f"case when NEW.[{name}] is null "
                f"then json_object('null', 1) "
                f"else NEW.[{name}] end"
            )
        json_args.append(f"'{name}', {val_expr}")

    json_obj = f"json_object({', '.join(json_args)})" if json_args else "'{{}}'"

    group_subquery = f"(select id from [{_GROUPS_TABLE}] where current = 1)"

    return f"""create trigger if not exists [{audit_name}_insert]
after insert on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_col_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'insert',
        {pk_new_refs},
        {json_obj},
        {group_subquery}
    );
end;"""


def _build_update_trigger_sql(
    table_name: str,
    audit_name: str,
    pk_cols: list[dict],
    non_pk_cols: list[dict],
) -> str:
    """Build the AFTER UPDATE trigger SQL using nested json_patch."""
    audit_pk_col_names = ", ".join(
        f"[{_audit_pk_col_name(c['name'])}]" for c in pk_cols
    )
    pk_new_refs = ", ".join(f"NEW.[{c['name']}]" for c in pk_cols)

    if not non_pk_cols:
        json_expr = "'{}'"
    else:
        def case_for_col(col: dict) -> str:
            name = col["name"]
            if _is_blob_type(col["type"]):
                return (
                    f"case\n"
                    f"                when OLD.[{name}] is not NEW.[{name}] then\n"
                    f"                    case\n"
                    f"                        when NEW.[{name}] is null then json_object('{name}', json_object('null', 1))\n"
                    f"                        else json_object('{name}', json_object('hex', hex(NEW.[{name}])))\n"
                    f"                    end\n"
                    f"                else '{{}}'\n"
                    f"            end"
                )
            else:
                return (
                    f"case\n"
                    f"                when OLD.[{name}] is not NEW.[{name}] then\n"
                    f"                    case\n"
                    f"                        when NEW.[{name}] is null then json_object('{name}', json_object('null', 1))\n"
                    f"                        else json_object('{name}', NEW.[{name}])\n"
                    f"                    end\n"
                    f"                else '{{}}'\n"
                    f"            end"
                )

        expr = "'{}'"
        for col in non_pk_cols:
            case = case_for_col(col)
            expr = f"json_patch(\n            {expr},\n            {case}\n        )"

        json_expr = expr

    group_subquery = f"(select id from [{_GROUPS_TABLE}] where current = 1)"

    return f"""create trigger if not exists [{audit_name}_update]
after update on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_col_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'update',
        {pk_new_refs},
        {json_expr},
        {group_subquery}
    );
end;"""


def _build_delete_trigger_sql(
    table_name: str,
    audit_name: str,
    pk_cols: list[dict],
) -> str:
    """Build the AFTER DELETE trigger SQL."""
    audit_pk_col_names = ", ".join(
        f"[{_audit_pk_col_name(c['name'])}]" for c in pk_cols
    )
    pk_old_refs = ", ".join(f"OLD.[{c['name']}]" for c in pk_cols)

    group_subquery = f"(select id from [{_GROUPS_TABLE}] where current = 1)"

    return f"""create trigger if not exists [{audit_name}_delete]
after delete on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_col_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'delete',
        {pk_old_refs},
        null,
        {group_subquery}
    );
end;"""


def enable_tracking(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    populate_table: bool = True,
    atomic: bool = True,
) -> None:
    """Create audit table and triggers for the given table.

    The audit table is named ``_history_json_{table_name}`` and contains:
    - id: auto-incrementing primary key
    - timestamp: ISO-8601 datetime with microsecond precision
    - operation: 'insert', 'update', or 'delete'
    - pk_{col} columns for each primary key column of the source table
    - updated_values: JSON text of changed column values

    Args:
        conn: SQLite connection.
        table_name: Name of the table to track.
        populate_table: If True (the default), snapshot all existing rows into
            the audit log so history is complete from this point.
        atomic: If True (the default), wrap setup and optional populate
            work in a SAVEPOINT so the operation is atomic.

    This is idempotent: calling it twice has no additional effect.
    """
    def _enable_tracking_inner() -> None:
        columns = _get_table_info(conn, table_name)
        pk_cols = _get_pk_columns(columns)
        non_pk_cols = _get_non_pk_columns(columns)
        audit_name = _audit_table_name(table_name)

        if not pk_cols:
            raise ValueError(
                f"Table {table_name!r} has no explicit primary key. "
                "sqlite-history-json requires an explicit PRIMARY KEY."
            )

        # Ensure the shared groups table exists (triggers reference it)
        _ensure_groups_table(conn)

        # Build audit table PK column definitions with pk_ prefix
        pk_col_defs = ", ".join(
            f"[{_audit_pk_col_name(c['name'])}] {c['type']}" for c in pk_cols
        )

        create_audit = f"""create table if not exists [{audit_name}] (
    id integer primary key,
    timestamp text,
    operation text,
    {pk_col_defs},
    updated_values text,
    [group] integer references [{_GROUPS_TABLE}](id)
);"""

        conn.execute(create_audit)

        # Build and create triggers
        insert_sql = _build_insert_trigger_sql(
            table_name, audit_name, pk_cols, non_pk_cols
        )
        update_sql = _build_update_trigger_sql(
            table_name, audit_name, pk_cols, non_pk_cols
        )
        delete_sql = _build_delete_trigger_sql(table_name, audit_name, pk_cols)

        conn.execute(insert_sql)
        conn.execute(update_sql)
        conn.execute(delete_sql)

        # Create indexes for common query patterns
        conn.execute(
            f"create index if not exists [{audit_name}_timestamp] "
            f"on [{audit_name}] (timestamp)"
        )
        audit_pk_col_names_str = ", ".join(
            f"[{_audit_pk_col_name(c['name'])}]" for c in pk_cols
        )
        conn.execute(
            f"create index if not exists [{audit_name}_pk] "
            f"on [{audit_name}] ({audit_pk_col_names_str})"
        )

        if populate_table:
            # Only populate if audit table is empty (preserves idempotency)
            row_count = conn.execute(
                f"select count(*) from [{audit_name}]"
            ).fetchone()[0]
            if row_count == 0:
                populate(conn, table_name)

    if atomic:
        _run_in_savepoint(conn, _enable_tracking_inner)
    else:
        _enable_tracking_inner()


def disable_tracking(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    atomic: bool = True,
) -> None:
    """Drop triggers for the given table. Keeps the audit table intact.

    This is idempotent: calling it when no triggers exist has no effect.

    Args:
        conn: SQLite connection.
        table_name: Name of the tracked table.
        atomic: If True (the default), wrap trigger drops in a SAVEPOINT
            so the operation is atomic.
    """
    def _disable_tracking_inner() -> None:
        audit_name = _audit_table_name(table_name)
        conn.execute(f"drop trigger if exists [{audit_name}_insert]")
        conn.execute(f"drop trigger if exists [{audit_name}_update]")
        conn.execute(f"drop trigger if exists [{audit_name}_delete]")

    if atomic:
        _run_in_savepoint(conn, _disable_tracking_inner)
    else:
        _disable_tracking_inner()


def populate(conn: sqlite3.Connection, table_name: str) -> None:
    """Populate the audit log with a snapshot of the current table state.

    For each existing row, creates an 'insert' entry in the audit log
    containing all column values. This makes the audit log self-contained
    for reconstruction purposes.

    The audit table and triggers must already exist (call enable_tracking first).
    """
    columns = _get_table_info(conn, table_name)
    pk_cols = _get_pk_columns(columns)
    non_pk_cols = _get_non_pk_columns(columns)
    audit_name = _audit_table_name(table_name)

    # Read all current rows
    all_col_names = ", ".join(f"[{c['name']}]" for c in columns)
    rows = conn.execute(f"select {all_col_names} from [{table_name}]").fetchall()

    for row in rows:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col["name"]] = row[i]

        # Build the PK values with pk_ prefix column names
        pk_insert_cols = ", ".join(
            f"[{_audit_pk_col_name(c['name'])}]" for c in pk_cols
        )
        pk_insert_params = ", ".join("?" for _ in pk_cols)
        pk_values = [row_dict[c["name"]] for c in pk_cols]

        # Build JSON for non-PK columns
        json_dict = {}
        for col in non_pk_cols:
            val = row_dict[col["name"]]
            if val is None:
                json_dict[col["name"]] = {"null": 1}
            elif isinstance(val, bytes):
                json_dict[col["name"]] = {"hex": val.hex().upper()}
            else:
                json_dict[col["name"]] = val

        json_str = json.dumps(json_dict)

        group_subquery = f"(select id from [{_GROUPS_TABLE}] where current = 1)"
        conn.execute(
            f"insert into [{audit_name}] (timestamp, operation, {pk_insert_cols}, updated_values, [group]) "
            f"values (strftime('%Y-%m-%d %H:%M:%f', 'now'), 'insert', {pk_insert_params}, ?, "
            f"{group_subquery})",
            pk_values + [json_str],
        )


def _decode_json_value(val):
    """Decode a JSON value from the audit log, handling null and hex conventions."""
    if isinstance(val, dict):
        if "null" in val:
            return None
        if "hex" in val:
            return bytes.fromhex(val["hex"])
    return val


def restore(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    timestamp: str | None = None,
    up_to_id: int | None = None,
    new_table_name: str | None = None,
    swap: bool = False,
) -> str:
    """Restore a table to its state at the given timestamp or audit entry ID.

    Replays audit log entries to reconstruct the table state. Filter by
    either ``timestamp`` (inclusive) or ``up_to_id`` (inclusive). If both
    are provided, both conditions must be satisfied.

    Args:
        conn: SQLite connection.
        table_name: Name of the tracked table.
        timestamp: ISO-8601 timestamp to restore to (inclusive).
        up_to_id: Audit log entry ID to restore up to (inclusive).
        new_table_name: Name for the restored table. If None, auto-generated.
        swap: If True, atomically swap the restored table with the original.

    Returns:
        The name of the restored table (equals table_name if swap=True).
    """
    columns = _get_table_info(conn, table_name)
    pk_cols = _get_pk_columns(columns)
    non_pk_cols = _get_non_pk_columns(columns)
    audit_name = _audit_table_name(table_name)

    if new_table_name is None and not swap:
        new_table_name = f"{table_name}_restored"

    target_name = new_table_name if not swap else f"_tmp_restore_{table_name}"

    # Create target table with same schema
    create_sql = conn.execute(
        "select sql from sqlite_master where type='table' and name=?",
        (table_name,),
    ).fetchone()[0]

    # Replace the table name in the CREATE statement
    target_create = create_sql.replace(
        f"CREATE TABLE {table_name}", f"CREATE TABLE [{target_name}]", 1
    )
    if target_create == create_sql:
        target_create = create_sql.replace(
            f'CREATE TABLE "{table_name}"', f"CREATE TABLE [{target_name}]", 1
        )
    if target_create == create_sql:
        target_create = create_sql.replace(
            f"CREATE TABLE [{table_name}]", f"CREATE TABLE [{target_name}]", 1
        )

    # Drop if already exists
    conn.execute(f"drop table if exists [{target_name}]")
    conn.execute(target_create)

    # Read audit log entries up to the specified point
    conditions = []
    params: list = []
    if timestamp is not None:
        conditions.append("timestamp <= ?")
        params.append(timestamp)
    if up_to_id is not None:
        conditions.append("id <= ?")
        params.append(up_to_id)
    where_clause = f" where {' and '.join(conditions)}" if conditions else ""
    audit_rows = conn.execute(
        f"select * from [{audit_name}]{where_clause} order by id",
        params,
    ).fetchall()

    # Get audit column names
    audit_col_names = [desc[0] for desc in conn.execute(
        f"select * from [{audit_name}] limit 0"
    ).description]

    for audit_row in audit_rows:
        row_dict = dict(zip(audit_col_names, audit_row))
        operation = row_dict["operation"]

        # Get PK values from audit row (pk_ prefixed columns)
        pk_where = " and ".join(f"[{c['name']}] = ?" for c in pk_cols)
        pk_values = [row_dict[_audit_pk_col_name(c["name"])] for c in pk_cols]

        if operation == "insert":
            updated_values = json.loads(row_dict["updated_values"])
            # Build full row: PK values + decoded non-PK values
            all_cols = []
            all_vals = []
            for col in pk_cols:
                all_cols.append(f"[{col['name']}]")
                all_vals.append(row_dict[_audit_pk_col_name(col["name"])])
            for col in non_pk_cols:
                all_cols.append(f"[{col['name']}]")
                if col["name"] in updated_values:
                    all_vals.append(_decode_json_value(updated_values[col["name"]]))
                else:
                    all_vals.append(None)

            placeholders = ", ".join("?" for _ in all_cols)
            conn.execute(
                f"insert into [{target_name}] ({', '.join(all_cols)}) values ({placeholders})",
                all_vals,
            )

        elif operation == "update":
            updated_values = json.loads(row_dict["updated_values"])
            if not updated_values:
                continue  # No actual changes
            set_clauses = []
            set_vals = []
            for col_name, val in updated_values.items():
                set_clauses.append(f"[{col_name}] = ?")
                set_vals.append(_decode_json_value(val))

            conn.execute(
                f"update [{target_name}] set {', '.join(set_clauses)} where {pk_where}",
                set_vals + pk_values,
            )

        elif operation == "delete":
            conn.execute(
                f"delete from [{target_name}] where {pk_where}",
                pk_values,
            )

    if swap:
        old_backup = f"_tmp_old_{table_name}"
        conn.execute(f"drop table if exists [{old_backup}]")
        conn.execute(f"alter table [{table_name}] rename to [{old_backup}]")
        conn.execute(f"alter table [{target_name}] rename to [{table_name}]")
        conn.execute(f"drop table [{old_backup}]")
        return table_name

    return target_name


def get_history(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    limit: int | None = None,
) -> list[dict]:
    """Return audit log entries for a table, newest first.

    Each entry is a dict with keys: id, timestamp, operation, pk, updated_values.
    The ``pk`` dict uses original column names (no ``pk_`` prefix).
    For deletes, ``updated_values`` is None.

    Args:
        conn: SQLite connection.
        table_name: Name of the tracked table.
        limit: Maximum number of entries to return.
    """
    columns = _get_table_info(conn, table_name)
    pk_cols = _get_pk_columns(columns)
    audit_name = _audit_table_name(table_name)

    sql = (
        f"select a.*, g.note as group_note from [{audit_name}] a "
        f"left join [{_GROUPS_TABLE}] g on a.[group] = g.id "
        f"order by a.id desc"
    )
    if limit is not None:
        sql += f" limit {int(limit)}"

    cursor = conn.execute(sql)
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(col_names, row))
        pk = {
            c["name"]: row_dict[_audit_pk_col_name(c["name"])] for c in pk_cols
        }
        updated_values = (
            json.loads(row_dict["updated_values"])
            if row_dict["updated_values"] is not None
            else None
        )
        result.append(
            {
                "id": row_dict["id"],
                "timestamp": row_dict["timestamp"],
                "operation": row_dict["operation"],
                "pk": pk,
                "updated_values": updated_values,
                "group": row_dict["group"],
                "group_note": row_dict["group_note"],
            }
        )
    return result


def get_row_history(
    conn: sqlite3.Connection,
    table_name: str,
    pk_values: dict[str, object],
    *,
    limit: int | None = None,
) -> list[dict]:
    """Return audit log entries for a specific row, newest first.

    Same format as :func:`get_history`, filtered by primary key values.

    Args:
        conn: SQLite connection.
        table_name: Name of the tracked table.
        pk_values: Dict mapping PK column names to their values,
            e.g. ``{"id": 1}`` or ``{"user_id": 1, "role_id": 2}``.
        limit: Maximum number of entries to return.
    """
    columns = _get_table_info(conn, table_name)
    pk_cols = _get_pk_columns(columns)
    audit_name = _audit_table_name(table_name)

    where_parts = []
    params: list = []
    for col in pk_cols:
        audit_col = _audit_pk_col_name(col["name"])
        where_parts.append(f"a.[{audit_col}] = ?")
        params.append(pk_values[col["name"]])

    sql = (
        f"select a.*, g.note as group_note from [{audit_name}] a "
        f"left join [{_GROUPS_TABLE}] g on a.[group] = g.id "
        f"where {' and '.join(where_parts)} "
        f"order by a.id desc"
    )
    if limit is not None:
        sql += f" limit {int(limit)}"

    cursor = conn.execute(sql, params)
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    result = []
    for row in rows:
        row_dict = dict(zip(col_names, row))
        pk = {
            c["name"]: row_dict[_audit_pk_col_name(c["name"])] for c in pk_cols
        }
        updated_values = (
            json.loads(row_dict["updated_values"])
            if row_dict["updated_values"] is not None
            else None
        )
        result.append(
            {
                "id": row_dict["id"],
                "timestamp": row_dict["timestamp"],
                "operation": row_dict["operation"],
                "pk": pk,
                "updated_values": updated_values,
                "group": row_dict["group"],
                "group_note": row_dict["group_note"],
            }
        )
    return result


def row_state_sql(
    conn: sqlite3.Connection,
    table_name: str,
) -> str:
    """Return a SQL query that reconstructs a row's state at a given audit version.

    The returned query uses ``json_patch()`` in a recursive CTE to fold
    all audit entries from the most recent insert through the target version.

    The query takes named parameters:

    - ``:pk`` for single-PK tables, or ``:pk_1``, ``:pk_2``, ... for
      compound PKs (numbered by PK column order).
    - ``:target_id`` — the audit log entry ID to reconstruct up to.

    The query returns a single row with one column (the JSON state),
    or no rows if the PK has no history at that version.  The JSON
    state is ``NULL`` if the row was deleted at that version.

    Args:
        conn: SQLite connection.
        table_name: Name of the tracked table.

    Raises:
        ValueError: If tracking is not enabled for the table.
    """
    audit_name = _audit_table_name(table_name)

    # Check that the audit table exists
    exists = conn.execute(
        "select count(*) from sqlite_master where type='table' and name=?",
        (audit_name,),
    ).fetchone()[0]
    if not exists:
        raise ValueError(
            f"Tracking is not enabled for table {table_name!r} "
            f"(audit table {audit_name!r} does not exist)."
        )

    columns = _get_table_info(conn, table_name)
    pk_cols = _get_pk_columns(columns)

    # Build PK parameter references and WHERE fragments
    if len(pk_cols) == 1:
        pk_params = {_audit_pk_col_name(pk_cols[0]["name"]): ":pk"}
    else:
        pk_params = {
            _audit_pk_col_name(c["name"]): f":pk_{i}"
            for i, c in enumerate(pk_cols, 1)
        }

    pk_where = " and ".join(
        f"[{col}] = {param}" for col, param in pk_params.items()
    )

    return (
        f"with entries as (\n"
        f"  select id, operation, updated_values,\n"
        f"         row_number() over (order by id) as rn\n"
        f"  from [{audit_name}]\n"
        f"  where {pk_where}\n"
        f"    and id <= :target_id\n"
        f"    and id >= (\n"
        f"      select max(id) from [{audit_name}]\n"
        f"      where {pk_where}\n"
        f"        and operation = 'insert' and id <= :target_id\n"
        f"    )\n"
        f"),\n"
        f"folded as (\n"
        f"  select rn, operation, updated_values as state\n"
        f"  from entries where rn = 1\n"
        f"\n"
        f"  union all\n"
        f"\n"
        f"  select e.rn, e.operation,\n"
        f"    case when e.operation = 'delete' then null\n"
        f"         else json_patch(f.state, e.updated_values)\n"
        f"    end\n"
        f"  from folded f\n"
        f"  join entries e on e.rn = f.rn + 1\n"
        f")\n"
        f"select state from folded order by rn desc limit 1"
    )
