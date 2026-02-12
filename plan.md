# Implementation Plan: POST /-/history-json Batch Request API

## Overview

Add a datasette plugin to sqlite-history-json that provides a `POST /-/history-json` endpoint. This endpoint accepts a list of sub-requests (insert, update, delete, upsert) and executes them all within a single `change_group`, so all audit log entries share the same group ID and note.

## API Design

### Request

```
POST /-/history-json
Content-Type: application/json
```

```json
{
  "requests": [
    {"method": "POST", "path": "/data/t/-/insert", "body": {"row": {"name": "Widget", "price": 10}}},
    {"method": "POST", "path": "/data/t/1/-/update", "body": {"update": {"price": 20}}},
    {"method": "POST", "path": "/data/t/2/-/delete"}
  ],
  "note": "Bulk update from import script"
}
```

Each sub-request has:
- `method` (required): HTTP method (typically "POST")
- `path` (required): Full datasette path including database and table
- `body` (optional): JSON body for the sub-request

### Response

```json
{
  "ok": true,
  "group_id": 5,
  "results": [
    {"status": 201, "body": {"ok": true}},
    {"status": 200, "body": {"ok": true}},
    {"status": 200, "body": {"ok": true}}
  ]
}
```

### Error Cases
- Invalid JSON or missing `requests` → 400
- All request paths must target the same database → 400
- Individual sub-request failures are captured in results (execution continues)

## Architecture

### How change_group works (recap)
1. INSERT a row into `_history_json` with `current=1` and the note
2. All audit triggers reference `(SELECT id FROM _history_json WHERE current = 1)` to get group ID
3. On cleanup, UPDATE `current=NULL`

### Batch execution strategy
1. Extract the target database from sub-request paths (validate all same DB)
2. Use `database.execute_write_fn()` to set up the change group (INSERT current=1)
3. Dispatch each sub-request via `datasette.client.request()` — these go through the full ASGI stack, execute on the write thread, and triggers see the active group
4. Clean up the change group (UPDATE current=NULL) in a `finally` block
5. Authentication headers/cookies from the original request are forwarded to sub-requests

### Why this works
- Datasette serializes all writes through a single write connection per database
- `execute_write_fn` for setup completes and commits before sub-requests run
- Sub-request writes see the committed `current=1` row via trigger subqueries
- Cleanup runs after all sub-requests, also via `execute_write_fn`

## Files to Create/Modify

### 1. NEW: `sqlite_history_json/datasette_plugin.py`
The datasette plugin module containing:
- `register_routes()` hookimpl → registers `/-/history-json`
- `batch_request_view()` async view function
- Helper functions for setup/cleanup of change groups

### 2. MODIFY: `pyproject.toml`
- Add `datasette` as optional dependency
- Add `[project.entry-points.datasette]` entry point pointing to the plugin module

### 3. NEW: `tests/test_datasette_plugin.py`
Tests covering:
- Basic batch insert — verify all rows inserted and share same change group
- Batch with mixed operations (insert + update + delete) sharing one group
- The `note` field is recorded on the change group
- Error handling: invalid JSON, empty requests, cross-database requests
- Individual sub-request failure doesn't prevent others from executing
- Authentication forwarding (cookies/headers)
