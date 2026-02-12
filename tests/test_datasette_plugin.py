"""Tests for the datasette batch request plugin (/-/history-json)."""

import json
import sqlite3

import pytest
import pytest_asyncio

from datasette.app import Datasette
from sqlite_history_json import enable_tracking, get_history


@pytest_asyncio.fixture
async def ds(tmp_path):
    """Datasette instance with a test database that has history tracking enabled."""
    db_path = str(tmp_path / "data.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)"
    )
    enable_tracking(conn, "items")
    conn.close()
    ds = Datasette([db_path])
    await ds.invoke_startup()
    return ds


@pytest_asyncio.fixture
async def ds_two_tables(tmp_path):
    """Datasette instance with two tracked tables."""
    db_path = str(tmp_path / "data.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)"
    )
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER, qty INTEGER)"
    )
    enable_tracking(conn, "items")
    enable_tracking(conn, "orders")
    conn.close()
    ds = Datasette([db_path])
    await ds.invoke_startup()
    return ds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def batch_request(ds, payload):
    """Send a batch request to /-/history-json."""
    return await ds.client.post(
        "/-/history-json",
        json=payload,
        skip_permission_checks=True,
    )


async def get_table_rows(ds, db_name, table):
    """Read all rows from a table."""
    response = await ds.client.get(f"/{db_name}/{table}.json?_shape=array")
    return response.json()


async def get_audit_history(ds, db_name, table):
    """Get audit history by reading the database directly."""
    db = ds.get_database(db_name)

    def _read(conn):
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT a.*, g.note as group_note "
            f"FROM _history_json_{table} a "
            f"LEFT JOIN _history_json g ON a.[group] = g.id "
            f"ORDER BY a.id"
        ).fetchall()
        return [dict(r) for r in rows]

    return await db.execute_fn(_read)


# ---------------------------------------------------------------------------
# RED: Tests for basic batch insert
# ---------------------------------------------------------------------------


class TestBatchInsert:
    @pytest.mark.asyncio
    async def test_batch_insert_returns_ok(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "single insert",
        })
        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True

    @pytest.mark.asyncio
    async def test_batch_insert_multiple_rows(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 2, "name": "Gadget", "price": 24.99}},
                },
            ],
            "note": "two inserts",
        })
        data = response.json()
        assert data["ok"] is True
        assert len(data["results"]) == 2
        assert data["results"][0]["status"] == 201
        assert data["results"][1]["status"] == 201

        # Verify rows exist
        rows = await get_table_rows(ds, "data", "items")
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_batch_returns_group_id(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "test group",
        })
        data = response.json()
        assert "group_id" in data
        assert isinstance(data["group_id"], int)


# ---------------------------------------------------------------------------
# RED: Tests for change group integration
# ---------------------------------------------------------------------------


class TestChangeGrouping:
    @pytest.mark.asyncio
    async def test_all_inserts_share_same_group(self, ds):
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 2, "name": "Gadget", "price": 24.99}},
                },
            ],
            "note": "grouped inserts",
        })
        history = await get_audit_history(ds, "data", "items")
        assert len(history) == 2
        # Both should have the same non-null group
        assert history[0]["group"] is not None
        assert history[0]["group"] == history[1]["group"]

    @pytest.mark.asyncio
    async def test_note_is_stored_on_group(self, ds):
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "my custom note",
        })
        history = await get_audit_history(ds, "data", "items")
        assert history[0]["group_note"] == "my custom note"

    @pytest.mark.asyncio
    async def test_group_cleared_after_batch(self, ds):
        """After a batch, the current marker should be cleared."""
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "test",
        })
        # Subsequent writes outside batch should have no group
        await ds.client.post(
            "/data/items/-/insert",
            json={"row": {"id": 2, "name": "Gadget", "price": 24.99}},
            skip_permission_checks=True,
        )
        history = await get_audit_history(ds, "data", "items")
        assert history[0]["group"] is not None  # from batch
        assert history[1]["group"] is None  # after batch

    @pytest.mark.asyncio
    async def test_separate_batches_get_different_groups(self, ds):
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "batch 1",
        })
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 2, "name": "Gadget", "price": 24.99}},
                },
            ],
            "note": "batch 2",
        })
        history = await get_audit_history(ds, "data", "items")
        assert history[0]["group"] != history[1]["group"]
        assert history[0]["group_note"] == "batch 1"
        assert history[1]["group_note"] == "batch 2"


# ---------------------------------------------------------------------------
# RED: Tests for mixed operations
# ---------------------------------------------------------------------------


class TestMixedOperations:
    @pytest.mark.asyncio
    async def test_insert_then_update(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/1/-/update",
                    "body": {"update": {"price": 19.99}},
                },
            ],
            "note": "insert and update",
        })
        data = response.json()
        assert data["ok"] is True
        assert len(data["results"]) == 2

        # Verify final state
        rows = await get_table_rows(ds, "data", "items")
        assert rows[0]["price"] == 19.99

        # All audit entries in same group
        history = await get_audit_history(ds, "data", "items")
        groups = {h["group"] for h in history}
        assert len(groups) == 1
        assert None not in groups

    @pytest.mark.asyncio
    async def test_insert_then_delete(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/1/-/delete",
                },
            ],
            "note": "insert and delete",
        })
        data = response.json()
        assert data["ok"] is True

        rows = await get_table_rows(ds, "data", "items")
        assert len(rows) == 0

        # Both operations in same group
        history = await get_audit_history(ds, "data", "items")
        assert len(history) == 2
        assert history[0]["group"] == history[1]["group"]

    @pytest.mark.asyncio
    async def test_cross_table_operations_same_group(self, ds_two_tables):
        """Operations on different tables in the same DB share the same group."""
        response = await batch_request(ds_two_tables, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/orders/-/insert",
                    "body": {"row": {"id": 1, "item_id": 1, "qty": 5}},
                },
            ],
            "note": "cross-table batch",
        })
        data = response.json()
        assert data["ok"] is True

        items_history = await get_audit_history(ds_two_tables, "data", "items")
        orders_history = await get_audit_history(ds_two_tables, "data", "orders")
        assert items_history[0]["group"] == orders_history[0]["group"]


# ---------------------------------------------------------------------------
# RED: Tests for error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_post_required(self, ds):
        response = await ds.client.get("/-/history-json")
        assert response.status_code == 405

    @pytest.mark.asyncio
    async def test_empty_requests_list(self, ds):
        response = await batch_request(ds, {
            "requests": [],
            "note": "empty",
        })
        data = response.json()
        assert data["ok"] is True
        assert data["results"] == []

    @pytest.mark.asyncio
    async def test_missing_requests_key(self, ds):
        response = await batch_request(ds, {
            "note": "no requests",
        })
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_subrequest_failure_captured(self, ds):
        """If a sub-request fails, its error is in results but others still run."""
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Duplicate", "price": 5.0}},
                },
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 2, "name": "Gadget", "price": 24.99}},
                },
            ],
            "note": "with failure",
        })
        data = response.json()
        assert data["ok"] is True
        assert len(data["results"]) == 3
        # First succeeds
        assert data["results"][0]["status"] == 201
        # Second fails (duplicate PK)
        assert data["results"][1]["status"] != 201
        # Third still succeeds
        assert data["results"][2]["status"] == 201

    @pytest.mark.asyncio
    async def test_group_cleaned_up_on_subrequest_failure(self, ds):
        """Even if a sub-request fails, the group marker should be cleaned up."""
        await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
                {
                    "method": "POST",
                    "path": "/data/nonexistent/-/insert",
                    "body": {"row": {"id": 1, "name": "Bad"}},
                },
            ],
            "note": "partial failure",
        })
        # The group should be cleaned up - subsequent writes have no group
        await ds.client.post(
            "/data/items/-/insert",
            json={"row": {"id": 2, "name": "Later", "price": 5.0}},
            skip_permission_checks=True,
        )
        history = await get_audit_history(ds, "data", "items")
        # Last entry should have no group
        assert history[-1]["group"] is None

    @pytest.mark.asyncio
    async def test_note_is_optional(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
        })
        data = response.json()
        assert data["ok"] is True

    @pytest.mark.asyncio
    async def test_body_is_optional_for_delete(self, ds):
        # First insert a row
        await ds.client.post(
            "/data/items/-/insert",
            json={"row": {"id": 1, "name": "Widget", "price": 9.99}},
            skip_permission_checks=True,
        )
        response = await batch_request(ds, {
            "requests": [
                {"method": "POST", "path": "/data/items/1/-/delete"},
            ],
            "note": "delete without body",
        })
        data = response.json()
        assert data["ok"] is True
        assert data["results"][0]["status"] == 200


# ---------------------------------------------------------------------------
# RED: Tests for results format
# ---------------------------------------------------------------------------


class TestResultsFormat:
    @pytest.mark.asyncio
    async def test_results_include_status_and_body(self, ds):
        response = await batch_request(ds, {
            "requests": [
                {
                    "method": "POST",
                    "path": "/data/items/-/insert",
                    "body": {"row": {"id": 1, "name": "Widget", "price": 9.99}},
                },
            ],
            "note": "test",
        })
        data = response.json()
        result = data["results"][0]
        assert "status" in result
        assert "body" in result
        assert result["body"]["ok"] is True
