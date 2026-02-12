"""Datasette plugin that provides a batch request API with change grouping.

Registers a ``POST /-/history-json`` endpoint that accepts a list of
sub-requests and executes them all within a single ``change_group``,
so every audit-log entry shares the same group id and note.
"""

from __future__ import annotations

import json
import sqlite3

from datasette import hookimpl
from datasette.utils.asgi import Response

from .core import _ensure_groups_table, _GROUPS_TABLE


def _extract_database(path: str) -> str | None:
    """Extract the database name from a datasette path like /db/table/-/action."""
    parts = path.strip("/").split("/")
    return parts[0] if parts else None


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

    # Set up the change group on the write connection
    def setup_group(conn):
        _ensure_groups_table(conn)
        conn.execute(
            f"UPDATE [{_GROUPS_TABLE}] SET current = NULL WHERE current = 1"
        )
        cursor = conn.execute(
            f"INSERT INTO [{_GROUPS_TABLE}] (note, current) VALUES (?, 1)",
            [note],
        )
        return cursor.lastrowid

    group_id = await db.execute_write_fn(setup_group)

    try:
        results = []
        for sr in sub_requests:
            method = sr.get("method", "POST").upper()
            path = sr.get("path", "")
            sub_body = sr.get("body")

            kwargs = {"skip_permission_checks": True}
            if sub_body is not None:
                kwargs["json"] = sub_body

            response = await datasette.client.request(method, path, **kwargs)

            # Parse response body
            content_type = response.headers.get("content-type", "")
            if "json" in content_type:
                try:
                    resp_body = response.json()
                except (json.JSONDecodeError, ValueError):
                    resp_body = response.text
            else:
                resp_body = response.text

            results.append({
                "status": response.status_code,
                "body": resp_body,
            })
    finally:
        # Always clean up the change group marker
        def cleanup_group(conn):
            conn.execute(
                f"UPDATE [{_GROUPS_TABLE}] SET current = NULL WHERE current = 1"
            )

        await db.execute_write_fn(cleanup_group)

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
