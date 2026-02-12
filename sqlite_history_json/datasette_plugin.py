"""Datasette plugin that provides a batch request API with change grouping.

Registers a ``POST /-/history-json`` endpoint that accepts a list of
sub-requests and executes them all within a single ``change_group``,
so every audit-log entry shares the same group id and note.

All sub-requests are executed in a single ``execute_write_fn`` call,
which means they run as one task on datasette's write thread — no
other write can interleave and accidentally inherit the change group.
"""

from __future__ import annotations

import json

from datasette import hookimpl
from datasette.utils.asgi import Response

from .core import _ensure_groups_table, _GROUPS_TABLE


def _extract_database(path: str) -> str | None:
    """Extract the database name from a datasette path like /db/table/-/action."""
    parts = path.strip("/").split("/")
    return parts[0] if parts else None


def _parse_sub_request(path: str):
    """Parse a datasette-style path into (table, operation, pk_path).

    Returns (table_name, operation, pk_path) where operation is one of
    'insert', 'upsert', 'update', 'delete' and pk_path is the raw
    primary-key segment (or None for insert/upsert).
    """
    parts = path.strip("/").split("/")
    # /db/table/-/insert       → ['db', 'table', '-', 'insert']
    # /db/table/pk/-/update    → ['db', 'table', 'pk', '-', 'update']
    # /db/table/pk/-/delete    → ['db', 'table', 'pk', '-', 'delete']
    # /db/table/-/upsert       → ['db', 'table', '-', 'upsert']
    if len(parts) >= 4 and parts[-2] == "-":
        operation = parts[-1]
        if operation in ("insert", "upsert"):
            table_name = "/".join(parts[1:-2])
            return table_name, operation, None
        elif operation in ("update", "delete"):
            table_name = parts[1]
            pk_path = "/".join(parts[2:-2])
            return table_name, operation, pk_path
    return None, None, None


def _coerce_pk_value(value_str: str, pk_columns: list[dict]):
    """Coerce a PK string from the URL into the right Python type(s).

    For a single PK, returns the scalar value.
    For compound PKs, the URL segment is comma-separated.
    """
    if len(pk_columns) == 1:
        return _coerce_single(value_str, pk_columns[0]["type"])
    # Compound PK: split on commas
    parts = value_str.split(",")
    return tuple(
        _coerce_single(p, col["type"])
        for p, col in zip(parts, pk_columns)
    )


def _coerce_single(value_str: str, col_type: str):
    """Coerce a single PK string value based on column type."""
    upper = col_type.upper()
    if "INT" in upper:
        try:
            return int(value_str)
        except ValueError:
            pass
    elif "REAL" in upper or "FLOAT" in upper or "DOUBLE" in upper:
        try:
            return float(value_str)
        except ValueError:
            pass
    return value_str


def _execute_sub_request(conn, table_name, operation, pk_path, body):
    """Execute a single sub-request using direct SQL via sqlite-utils.

    Returns a result dict with 'status' and 'body'.
    """
    import sqlite_utils

    sdb = sqlite_utils.Database(conn)
    table = sdb[table_name]
    body = body or {}

    if operation == "insert":
        if "row" in body:
            table.insert(body["row"], replace=body.get("replace", False),
                         ignore=body.get("ignore", False))
            return {"status": 201, "body": {"ok": True}}
        elif "rows" in body:
            table.insert_all(body["rows"], replace=body.get("replace", False),
                             ignore=body.get("ignore", False))
            return {"status": 201, "body": {"ok": True}}
        else:
            return {"status": 400, "body": {"ok": False, "errors": ["Missing 'row' or 'rows'"]}}

    elif operation == "upsert":
        pks = table.pks
        pk = pks[0] if len(pks) == 1 else pks
        if "row" in body:
            table.upsert(body["row"], pk=pk)
            return {"status": 201, "body": {"ok": True}}
        elif "rows" in body:
            table.upsert_all(body["rows"], pk=pk)
            return {"status": 201, "body": {"ok": True}}
        else:
            return {"status": 400, "body": {"ok": False, "errors": ["Missing 'row' or 'rows'"]}}

    elif operation == "update":
        pk_columns = [
            {"name": col.name, "type": col.type}
            for col in table.columns if col.is_pk
        ]
        pk_value = _coerce_pk_value(pk_path, pk_columns)
        updates = body.get("update", {})
        if not updates:
            return {"status": 400, "body": {"ok": False, "errors": ["Missing 'update'"]}}
        table.update(pk_value, updates)
        return {"status": 200, "body": {"ok": True}}

    elif operation == "delete":
        pk_columns = [
            {"name": col.name, "type": col.type}
            for col in table.columns if col.is_pk
        ]
        pk_value = _coerce_pk_value(pk_path, pk_columns)
        table.delete(pk_value)
        return {"status": 200, "body": {"ok": True}}

    return {"status": 400, "body": {"ok": False, "errors": [f"Unknown operation: {operation}"]}}


async def _batch_request_view(datasette, request):
    if request.method != "POST":
        return Response.json(
            {"ok": False, "error": "Method not allowed"},
            status=405,
        )

    try:
        body = json.loads(await request.post_body())
    except (json.JSONDecodeError, ValueError) as e:
        return Response.json(
            {"ok": False, "error": f"Invalid JSON: {e}"},
            status=400,
        )

    if "requests" not in body:
        return Response.json(
            {"ok": False, "error": "Missing 'requests' key"},
            status=400,
        )

    sub_requests = body["requests"]
    note = body.get("note")

    if not sub_requests:
        return Response.json({"ok": True, "group_id": None, "results": []})

    # Determine the target database from sub-request paths
    databases = set()
    for sr in sub_requests:
        db_name = _extract_database(sr.get("path", ""))
        if db_name:
            databases.add(db_name)

    if len(databases) != 1:
        return Response.json(
            {"ok": False, "error": "All requests must target the same database"},
            status=400,
        )

    db_name = databases.pop()

    try:
        db = datasette.get_database(db_name)
    except KeyError:
        return Response.json(
            {"ok": False, "error": f"Database '{db_name}' not found"},
            status=400,
        )

    # Parse all sub-requests upfront so we can fail fast on bad paths
    parsed = []
    for sr in sub_requests:
        table_name, operation, pk_path = _parse_sub_request(sr.get("path", ""))
        if operation is None:
            return Response.json(
                {"ok": False, "error": f"Cannot parse path: {sr.get('path', '')}"},
                status=400,
            )
        parsed.append((table_name, operation, pk_path, sr.get("body")))

    def do_batch(conn):
        _ensure_groups_table(conn)
        cursor = conn.execute(
            f"INSERT INTO [{_GROUPS_TABLE}] (note, current) VALUES (?, NULL)",
            [note],
        )
        group_id = cursor.lastrowid

        # Set current=1 — safe because we're the only task on the write thread
        conn.execute(
            f"UPDATE [{_GROUPS_TABLE}] SET current = 1 WHERE id = ?",
            [group_id],
        )

        results = []
        try:
            for table_name, operation, pk_path, sub_body in parsed:
                try:
                    result = _execute_sub_request(
                        conn, table_name, operation, pk_path, sub_body
                    )
                    results.append(result)
                except Exception as e:
                    results.append({
                        "status": 400,
                        "body": {"ok": False, "errors": [str(e)]},
                    })
        finally:
            # Clear current marker — triggers can no longer pick it up
            conn.execute(
                f"UPDATE [{_GROUPS_TABLE}] SET current = NULL WHERE id = ?",
                [group_id],
            )

        return group_id, results

    group_id, results = await db.execute_write_fn(do_batch)

    return Response.json({
        "ok": True,
        "group_id": group_id,
        "results": results,
    })


@hookimpl
def register_routes(datasette):
    return [
        (r"^/-/history-json$", _batch_request_view),
    ]
