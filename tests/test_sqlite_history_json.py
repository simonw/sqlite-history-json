"""Comprehensive tests for sqlite-history-json."""

import json
import sqlite3

import pytest

from sqlite_history_json import (
    change_group,
    disable_tracking,
    enable_tracking,
    get_history,
    get_row_history,
    populate,
    restore,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """In-memory SQLite database with JSON1 support."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    yield db
    db.close()


@pytest.fixture
def simple_table(conn):
    """Create a simple table with a single integer PK."""
    conn.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price FLOAT,
            quantity INTEGER
        )
        """
    )
    return conn


@pytest.fixture
def simple_table_with_data(simple_table):
    """Simple table pre-populated with some rows."""
    simple_table.executemany(
        "INSERT INTO items (id, name, price, quantity) VALUES (?, ?, ?, ?)",
        [
            (1, "Widget", 9.99, 100),
            (2, "Gadget", 24.99, 50),
            (3, "Doohickey", 4.99, 200),
        ],
    )
    return simple_table


@pytest.fixture
def compound_pk_table(conn):
    """Create a table with a compound primary key."""
    conn.execute(
        """
        CREATE TABLE user_roles (
            user_id INTEGER,
            role_id INTEGER,
            granted_by TEXT,
            active INTEGER,
            PRIMARY KEY (user_id, role_id)
        )
        """
    )
    return conn


@pytest.fixture
def blob_table(conn):
    """Create a table with a BLOB column."""
    conn.execute(
        """
        CREATE TABLE files (
            id INTEGER PRIMARY KEY,
            name TEXT,
            content BLOB
        )
        """
    )
    return conn


@pytest.fixture
def text_pk_table(conn):
    """Create a table with a TEXT primary key."""
    conn.execute(
        """
        CREATE TABLE config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    return conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def audit_table_name(table_name: str) -> str:
    return f"_history_json_{table_name}"


def get_audit_rows(conn, table_name: str) -> list[dict]:
    """Return all rows from the audit table as dicts."""
    name = audit_table_name(table_name)
    rows = conn.execute(f"SELECT * FROM [{name}] ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row[0] > 0


def trigger_names(conn, table_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
        (table_name,),
    ).fetchall()
    return sorted(r[0] for r in rows)


def index_names(conn, table_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
        (table_name,),
    ).fetchall()
    return sorted(r[0] for r in rows)


# ---------------------------------------------------------------------------
# Tests: enable_tracking
# ---------------------------------------------------------------------------


class TestEnableTracking:
    def test_creates_audit_table(self, simple_table):
        enable_tracking(simple_table, "items")
        assert table_exists(simple_table, "_history_json_items")

    def test_audit_table_has_pk_columns(self, simple_table):
        enable_tracking(simple_table, "items")
        info = simple_table.execute(
            "PRAGMA table_info(_history_json_items)"
        ).fetchall()
        col_names = [r[1] for r in info]
        assert "id" in col_names  # audit table's own PK
        assert "timestamp" in col_names
        assert "operation" in col_names
        assert "pk_id" in col_names  # source table's PK with pk_ prefix
        assert "updated_values" in col_names

    def test_audit_table_compound_pk_columns(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        info = compound_pk_table.execute(
            "PRAGMA table_info(_history_json_user_roles)"
        ).fetchall()
        col_names = [r[1] for r in info]
        assert "pk_user_id" in col_names
        assert "pk_role_id" in col_names

    def test_creates_triggers(self, simple_table):
        enable_tracking(simple_table, "items")
        triggers = trigger_names(simple_table, "items")
        # Should have insert, update, and delete triggers
        assert len(triggers) == 3
        assert any("insert" in t for t in triggers)
        assert any("update" in t for t in triggers)
        assert any("delete" in t for t in triggers)

    def test_trigger_names_are_versioned(self, simple_table):
        """Trigger names should include a version number."""
        from sqlite_history_json.core import _TRIGGER_VERSION

        enable_tracking(simple_table, "items")
        triggers = trigger_names(simple_table, "items")
        expected = sorted([
            f"history_json_v{_TRIGGER_VERSION}_insert_items",
            f"history_json_v{_TRIGGER_VERSION}_update_items",
            f"history_json_v{_TRIGGER_VERSION}_delete_items",
        ])
        assert triggers == expected

    def test_trigger_names_versioned_compound_pk(self, compound_pk_table):
        """Versioned trigger names work for compound PK tables."""
        from sqlite_history_json.core import _TRIGGER_VERSION

        enable_tracking(compound_pk_table, "user_roles")
        triggers = trigger_names(compound_pk_table, "user_roles")
        expected = sorted([
            f"history_json_v{_TRIGGER_VERSION}_insert_user_roles",
            f"history_json_v{_TRIGGER_VERSION}_update_user_roles",
            f"history_json_v{_TRIGGER_VERSION}_delete_user_roles",
        ])
        assert triggers == expected

    def test_trigger_names_versioned_special_chars(self, conn):
        """Versioned trigger names work for tables with special characters."""
        from sqlite_history_json.core import _TRIGGER_VERSION

        conn.execute(
            'CREATE TABLE "my-table" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        enable_tracking(conn, "my-table")
        triggers = trigger_names(conn, "my-table")
        expected = sorted([
            f"history_json_v{_TRIGGER_VERSION}_insert_my-table",
            f"history_json_v{_TRIGGER_VERSION}_update_my-table",
            f"history_json_v{_TRIGGER_VERSION}_delete_my-table",
        ])
        assert triggers == expected

    def test_disable_tracking_drops_versioned_triggers(self, simple_table):
        """disable_tracking should drop the versioned triggers."""
        enable_tracking(simple_table, "items")
        assert len(trigger_names(simple_table, "items")) == 3
        disable_tracking(simple_table, "items")
        assert len(trigger_names(simple_table, "items")) == 0

    def test_creates_indexes(self, simple_table):
        enable_tracking(simple_table, "items")
        audit_name = "_history_json_items"
        indexes = index_names(simple_table, audit_name)
        # Should have at least a timestamp index and a row_id index
        assert len(indexes) >= 2
        # Check that index names reference the audit table
        assert any("timestamp" in idx for idx in indexes)

    def test_creates_indexes_compound_pk(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        audit_name = "_history_json_user_roles"
        indexes = index_names(compound_pk_table, audit_name)
        assert len(indexes) >= 2

    def test_idempotent_call(self, simple_table):
        """Calling enable_tracking twice should not error."""
        enable_tracking(simple_table, "items")
        enable_tracking(simple_table, "items")
        assert table_exists(simple_table, "_history_json_items")

    def test_text_pk(self, text_pk_table):
        """Tables with TEXT primary keys should work."""
        enable_tracking(text_pk_table, "config")
        assert table_exists(text_pk_table, "_history_json_config")


# ---------------------------------------------------------------------------
# Tests: INSERT trigger
# ---------------------------------------------------------------------------


class TestInsertTrigger:
    def test_insert_records_all_values(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 1
        row = rows[0]
        assert row["operation"] == "insert"
        assert row["pk_id"] == 1
        vals = json.loads(row["updated_values"])
        assert vals["name"] == "Widget"
        assert vals["price"] == 9.99
        assert vals["quantity"] == 100

    def test_insert_null_values(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        rows = get_audit_rows(simple_table, "items")
        vals = json.loads(rows[0]["updated_values"])
        # price and quantity should be recorded as null
        assert vals["price"] == {"null": 1}
        assert vals["quantity"] == {"null": 1}

    def test_insert_blob(self, blob_table):
        enable_tracking(blob_table, "files")
        blob_table.execute(
            "INSERT INTO files (id, name, content) VALUES (1, 'test.bin', x'DEADBEEF')"
        )
        rows = get_audit_rows(blob_table, "files")
        vals = json.loads(rows[0]["updated_values"])
        assert vals["content"] == {"hex": "DEADBEEF"}

    def test_insert_compound_pk(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles (user_id, role_id, granted_by, active) VALUES (1, 2, 'admin', 1)"
        )
        rows = get_audit_rows(compound_pk_table, "user_roles")
        assert len(rows) == 1
        assert rows[0]["pk_user_id"] == 1
        assert rows[0]["pk_role_id"] == 2
        vals = json.loads(rows[0]["updated_values"])
        assert vals["granted_by"] == "admin"
        assert vals["active"] == 1

    def test_multiple_inserts(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'A', 1.0, 10)"
        )
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'B', 2.0, 20)"
        )
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 2
        assert rows[0]["pk_id"] == 1
        assert rows[1]["pk_id"] == 2


# ---------------------------------------------------------------------------
# Tests: UPDATE trigger
# ---------------------------------------------------------------------------


class TestUpdateTrigger:
    def test_update_records_changed_columns_only(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 2  # insert + update
        update_row = rows[1]
        assert update_row["operation"] == "update"
        vals = json.loads(update_row["updated_values"])
        assert vals == {"name": "Gizmo"}

    def test_update_multiple_columns(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute(
            "UPDATE items SET name = 'Gizmo', price = 19.99 WHERE id = 1"
        )
        rows = get_audit_rows(simple_table, "items")
        vals = json.loads(rows[1]["updated_values"])
        assert vals["name"] == "Gizmo"
        assert vals["price"] == 19.99
        assert "quantity" not in vals

    def test_update_to_null(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("UPDATE items SET price = NULL WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        vals = json.loads(rows[1]["updated_values"])
        assert vals["price"] == {"null": 1}

    def test_update_from_null(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        simple_table.execute("UPDATE items SET price = 5.99 WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        vals = json.loads(rows[1]["updated_values"])
        assert vals["price"] == 5.99

    def test_update_no_change_skipped(self, simple_table):
        """Updating a row to the same values should NOT create an audit entry."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute(
            "UPDATE items SET name = 'Widget' WHERE id = 1"
        )
        rows = get_audit_rows(simple_table, "items")
        # No-op update should be skipped entirely
        assert len(rows) == 1  # only the insert

    def test_update_no_change_all_columns_skipped(self, simple_table):
        """Setting all columns to their current values should NOT create an audit entry."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute(
            "UPDATE items SET name = 'Widget', price = 9.99, quantity = 100 WHERE id = 1"
        )
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 1  # only the insert

    def test_update_no_change_null_to_null_skipped(self, simple_table):
        """Updating NULL to NULL should NOT create an audit entry."""
        enable_tracking(simple_table, "items")
        simple_table.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        # price and quantity are already NULL
        simple_table.execute(
            "UPDATE items SET price = NULL WHERE id = 1"
        )
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 1  # only the insert

    def test_update_partial_change_still_recorded(self, simple_table):
        """If at least one column changes, the update IS recorded."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        # name stays the same, but price changes
        simple_table.execute(
            "UPDATE items SET name = 'Widget', price = 19.99 WHERE id = 1"
        )
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 2  # insert + update
        vals = json.loads(rows[1]["updated_values"])
        assert vals == {"price": 19.99}

    def test_update_blob(self, blob_table):
        enable_tracking(blob_table, "files")
        blob_table.execute(
            "INSERT INTO files (id, name, content) VALUES (1, 'a.bin', x'AABB')"
        )
        blob_table.execute("UPDATE files SET content = x'CCDD' WHERE id = 1")
        rows = get_audit_rows(blob_table, "files")
        vals = json.loads(rows[1]["updated_values"])
        assert vals["content"] == {"hex": "CCDD"}

    def test_update_compound_pk(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        compound_pk_table.execute(
            "UPDATE user_roles SET active = 0 WHERE user_id = 1 AND role_id = 2"
        )
        rows = get_audit_rows(compound_pk_table, "user_roles")
        assert rows[1]["pk_user_id"] == 1
        assert rows[1]["pk_role_id"] == 2
        vals = json.loads(rows[1]["updated_values"])
        assert vals == {"active": 0}


# ---------------------------------------------------------------------------
# Tests: DELETE trigger
# ---------------------------------------------------------------------------


class TestDeleteTrigger:
    def test_delete_records_operation(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("DELETE FROM items WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 2  # insert + delete
        delete_row = rows[1]
        assert delete_row["operation"] == "delete"
        assert delete_row["pk_id"] == 1

    def test_delete_updated_values_is_null_or_empty(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("DELETE FROM items WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        delete_row = rows[1]
        # updated_values should be NULL for deletes
        assert delete_row["updated_values"] is None

    def test_delete_compound_pk(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        compound_pk_table.execute(
            "DELETE FROM user_roles WHERE user_id = 1 AND role_id = 2"
        )
        rows = get_audit_rows(compound_pk_table, "user_roles")
        delete_row = rows[1]
        assert delete_row["operation"] == "delete"
        assert delete_row["pk_user_id"] == 1
        assert delete_row["pk_role_id"] == 2


# ---------------------------------------------------------------------------
# Tests: disable_tracking
# ---------------------------------------------------------------------------


class TestDisableTracking:
    def test_removes_triggers(self, simple_table):
        enable_tracking(simple_table, "items")
        disable_tracking(simple_table, "items")
        triggers = trigger_names(simple_table, "items")
        assert len(triggers) == 0

    def test_keeps_audit_table(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        disable_tracking(simple_table, "items")
        # Audit table should still exist with data
        assert table_exists(simple_table, "_history_json_items")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 1

    def test_no_tracking_after_disable(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        disable_tracking(simple_table, "items")
        # This should NOT create an audit entry
        simple_table.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 1  # still only the insert

    def test_disable_idempotent(self, simple_table):
        """Disabling when not enabled should not error."""
        enable_tracking(simple_table, "items")
        disable_tracking(simple_table, "items")
        disable_tracking(simple_table, "items")


# ---------------------------------------------------------------------------
# Tests: transaction wrapping
# ---------------------------------------------------------------------------


class TestTransactionWrapping:
    def test_enable_tracking_default_uses_transaction(self, simple_table):
        conn = simple_table
        conn.execute("BEGIN")
        try:
            enable_tracking(conn, "items")
            conn.execute("ROLLBACK")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        assert not table_exists(conn, "_history_json_items")
        assert trigger_names(conn, "items") == []

    def test_enable_tracking_default_nests_inside_outer_transaction(self, simple_table):
        conn = simple_table
        conn.execute("BEGIN")
        try:
            enable_tracking(conn, "items")
            # Should work before outer transaction commits.
            conn.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
            conn.execute("ROLLBACK")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        assert not table_exists(conn, "_history_json_items")

    def test_enable_tracking_atomic_false_does_not_start_savepoint(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items", atomic=False)
        assert table_exists(conn, "_history_json_items")

    def test_disable_tracking_default_uses_transaction(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute("BEGIN")
        try:
            disable_tracking(conn, "items")
            conn.execute("ROLLBACK")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        # Trigger drops should have been rolled back with outer transaction.
        assert len(trigger_names(conn, "items")) == 3


# ---------------------------------------------------------------------------
# Tests: populate
# ---------------------------------------------------------------------------


class TestPopulate:
    def test_populates_existing_rows(self, simple_table_with_data):
        conn = simple_table_with_data
        enable_tracking(conn, "items", populate_table=False)
        populate(conn, "items")
        rows = get_audit_rows(conn, "items")
        assert len(rows) == 3
        for row in rows:
            assert row["operation"] == "insert"

    def test_populate_values_match_current_state(self, simple_table_with_data):
        conn = simple_table_with_data
        enable_tracking(conn, "items", populate_table=False)
        populate(conn, "items")
        rows = get_audit_rows(conn, "items")
        vals = json.loads(rows[0]["updated_values"])
        assert vals["name"] == "Widget"
        assert vals["price"] == 9.99
        assert vals["quantity"] == 100

    def test_populate_compound_pk(self, compound_pk_table):
        conn = compound_pk_table
        conn.executemany(
            "INSERT INTO user_roles VALUES (?, ?, ?, ?)",
            [(1, 2, "admin", 1), (3, 4, "system", 0)],
        )
        enable_tracking(conn, "user_roles", populate_table=False)
        populate(conn, "user_roles")
        rows = get_audit_rows(conn, "user_roles")
        assert len(rows) == 2
        pk_pairs = [(r["pk_user_id"], r["pk_role_id"]) for r in rows]
        assert (1, 2) in pk_pairs
        assert (3, 4) in pk_pairs

    def test_populate_with_nulls(self, simple_table):
        conn = simple_table
        conn.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        enable_tracking(conn, "items", populate_table=False)
        populate(conn, "items")
        rows = get_audit_rows(conn, "items")
        vals = json.loads(rows[0]["updated_values"])
        assert vals["price"] == {"null": 1}
        assert vals["quantity"] == {"null": 1}

    def test_populate_with_blobs(self, blob_table):
        conn = blob_table
        conn.execute(
            "INSERT INTO files (id, name, content) VALUES (1, 'a.bin', x'CAFE')"
        )
        enable_tracking(conn, "files", populate_table=False)
        populate(conn, "files")
        rows = get_audit_rows(conn, "files")
        vals = json.loads(rows[0]["updated_values"])
        assert vals["content"] == {"hex": "CAFE"}

    def test_populate_empty_table(self, simple_table):
        enable_tracking(simple_table, "items")
        populate(simple_table, "items")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Tests: restore
# ---------------------------------------------------------------------------


class TestRestore:
    def test_restore_creates_new_table(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        populate(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        assert table_exists(conn, result_name)
        assert result_name != "items"

    def test_restore_default_table_name(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        assert "items" in result_name
        assert result_name != "items"

    def test_restore_custom_table_name(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        result_name = restore(
            conn, "items", timestamp="9999-12-31 23:59:59", new_table_name="items_copy"
        )
        assert result_name == "items_copy"
        assert table_exists(conn, "items_copy")

    def test_restore_replays_inserts(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
        )
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(
            f"SELECT * FROM [{result_name}] ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert dict(rows[0])["name"] == "Widget"
        assert dict(rows[1])["name"] == "Gadget"

    def test_restore_replays_updates(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo', price = 19.99 WHERE id = 1")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["name"] == "Gizmo"
        assert dict(row)["price"] == 19.99
        assert dict(row)["quantity"] == 100  # unchanged

    def test_restore_replays_deletes(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
        )
        conn.execute("DELETE FROM items WHERE id = 1")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(
            f"SELECT * FROM [{result_name}] ORDER BY id"
        ).fetchall()
        assert len(rows) == 1
        assert dict(rows[0])["id"] == 2

    def test_restore_to_earlier_point(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        # Get the audit log entry id of the insert
        audit_rows = get_audit_rows(conn, "items")
        insert_id = audit_rows[0]["id"]

        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        # Restore to just after the insert (before the update) using up_to_id
        result_name = restore(conn, "items", up_to_id=insert_id)
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["name"] == "Widget"

    def test_restore_null_handling(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["price"] is None
        assert dict(row)["quantity"] is None

    def test_restore_update_to_null(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET price = NULL WHERE id = 1")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["price"] is None

    def test_restore_blob_handling(self, blob_table):
        conn = blob_table
        enable_tracking(conn, "files")
        conn.execute(
            "INSERT INTO files (id, name, content) VALUES (1, 'a.bin', x'DEADBEEF')"
        )
        result_name = restore(conn, "files", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["content"] == b"\xde\xad\xbe\xef"

    def test_restore_compound_pk(self, compound_pk_table):
        conn = compound_pk_table
        enable_tracking(conn, "user_roles")
        conn.execute("INSERT INTO user_roles VALUES (1, 2, 'admin', 1)")
        conn.execute("INSERT INTO user_roles VALUES (3, 4, 'system', 0)")
        conn.execute(
            "UPDATE user_roles SET active = 0 WHERE user_id = 1 AND role_id = 2"
        )
        result_name = restore(conn, "user_roles", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE user_id = 1 AND role_id = 2"
        ).fetchone()
        assert dict(row)["active"] == 0
        assert dict(row)["granted_by"] == "admin"

    def test_restore_swap(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59", swap=True)
        assert result_name == "items"
        # The original table should now have the restored data
        row = conn.execute("SELECT * FROM items WHERE id = 1").fetchone()
        assert dict(row)["name"] == "Gizmo"

    def test_restore_swap_replaces_original(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        # Get audit entry id
        audit_rows = get_audit_rows(conn, "items")
        insert_id = audit_rows[0]["id"]

        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        # Restore to before the update, swapping in place
        restore(conn, "items", up_to_id=insert_id, swap=True)
        row = conn.execute("SELECT * FROM items WHERE id = 1").fetchone()
        assert dict(row)["name"] == "Widget"

    def test_restore_from_populated_data(self, simple_table_with_data):
        """Restore should work when audit log was populated from existing data."""
        conn = simple_table_with_data
        enable_tracking(conn, "items", populate_table=False)
        populate(conn, "items")
        # Make some changes
        conn.execute("UPDATE items SET name = 'Changed' WHERE id = 1")
        conn.execute("DELETE FROM items WHERE id = 2")

        # Get the audit entry id of the last populate entry (3rd entry)
        audit_rows = get_audit_rows(conn, "items")
        # The populate entries come first (3 rows), then the update and delete
        populate_last_id = audit_rows[2]["id"]

        result_name = restore(conn, "items", up_to_id=populate_last_id)
        rows = conn.execute(
            f"SELECT * FROM [{result_name}] ORDER BY id"
        ).fetchall()
        assert len(rows) == 3
        assert dict(rows[0])["name"] == "Widget"
        assert dict(rows[1])["name"] == "Gadget"
        assert dict(rows[2])["name"] == "Doohickey"

    def test_restore_empty_history(self, simple_table):
        """Restoring with no audit entries should yield an empty table."""
        conn = simple_table
        enable_tracking(conn, "items")
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(
            f"SELECT * FROM [{result_name}]"
        ).fetchall()
        assert len(rows) == 0

    def test_restore_text_pk(self, text_pk_table):
        conn = text_pk_table
        enable_tracking(conn, "config")
        conn.execute("INSERT INTO config VALUES ('theme', 'dark')")
        conn.execute("UPDATE config SET value = 'light' WHERE key = 'theme'")
        result_name = restore(conn, "config", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE key = 'theme'"
        ).fetchone()
        assert dict(row)["value"] == "light"


# ---------------------------------------------------------------------------
# Tests: parameterized (various column types)
# ---------------------------------------------------------------------------


COLUMN_TYPE_CASES = [
    ("text_val TEXT", "'hello'", "hello"),
    ("int_val INTEGER", "42", 42),
    ("float_val FLOAT", "3.14", 3.14),
    ("real_val REAL", "2.718", 2.718),
]


@pytest.mark.parametrize("col_def,sql_val,expected", COLUMN_TYPE_CASES)
def test_insert_various_types(conn, col_def, sql_val, expected):
    col_name = col_def.split()[0]
    conn.execute(f"CREATE TABLE typed (id INTEGER PRIMARY KEY, {col_def})")
    enable_tracking(conn, "typed")
    conn.execute(f"INSERT INTO typed (id, {col_name}) VALUES (1, {sql_val})")
    rows = get_audit_rows(conn, "typed")
    vals = json.loads(rows[0]["updated_values"])
    assert vals[col_name] == expected


@pytest.mark.parametrize("col_def,sql_val,expected", COLUMN_TYPE_CASES)
def test_restore_various_types(conn, col_def, sql_val, expected):
    col_name = col_def.split()[0]
    conn.execute(f"CREATE TABLE typed (id INTEGER PRIMARY KEY, {col_def})")
    enable_tracking(conn, "typed")
    conn.execute(f"INSERT INTO typed (id, {col_name}) VALUES (1, {sql_val})")
    result_name = restore(conn, "typed", timestamp="9999-12-31 23:59:59")
    row = conn.execute(f"SELECT * FROM [{result_name}] WHERE id = 1").fetchone()
    assert dict(row)[col_name] == expected


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_table_with_hyphen_in_name(self, conn):
        conn.execute(
            'CREATE TABLE "my-table" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        enable_tracking(conn, "my-table")
        assert table_exists(conn, "_history_json_my-table")

    def test_table_with_spaces_in_name(self, conn):
        """Tables with spaces in names should work end-to-end."""
        conn.execute(
            'CREATE TABLE "my cool table" (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)'
        )
        enable_tracking(conn, "my cool table")
        assert table_exists(conn, "_history_json_my cool table")

        conn.execute(
            'INSERT INTO "my cool table" (id, name, score) VALUES (1, \'Alice\', 95.5)'
        )
        conn.execute(
            'UPDATE "my cool table" SET score = 98.0 WHERE id = 1'
        )
        conn.execute(
            'DELETE FROM "my cool table" WHERE id = 1'
        )
        rows = get_audit_rows(conn, "my cool table")
        assert len(rows) == 3
        assert rows[0]["operation"] == "insert"
        assert rows[1]["operation"] == "update"
        assert rows[2]["operation"] == "delete"

    def test_table_with_spaces_full_lifecycle(self, conn):
        """Full enable/populate/restore cycle with spaces in table name."""
        conn.execute(
            'CREATE TABLE "order items" (id INTEGER PRIMARY KEY, product TEXT, qty INTEGER)'
        )
        conn.executemany(
            'INSERT INTO "order items" (id, product, qty) VALUES (?, ?, ?)',
            [(1, "Widget", 10), (2, "Gadget", 5)],
        )
        enable_tracking(conn, "order items", populate_table=False)
        populate(conn, "order items")
        conn.execute('UPDATE "order items" SET qty = 20 WHERE id = 1')
        conn.execute('DELETE FROM "order items" WHERE id = 2')

        # Restore after populate (before changes)
        audit_rows = get_audit_rows(conn, "order items")
        populate_last = audit_rows[1]["id"]  # 2 populate entries
        result = restore(conn, "order items", up_to_id=populate_last)
        rows = conn.execute(f'SELECT * FROM [{result}] ORDER BY id').fetchall()
        assert len(rows) == 2
        assert dict(rows[0])["product"] == "Widget"
        assert dict(rows[0])["qty"] == 10
        assert dict(rows[1])["product"] == "Gadget"

    def test_table_with_dots_in_name(self, conn):
        """Table names with dots."""
        conn.execute(
            'CREATE TABLE "schema.table" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        enable_tracking(conn, "schema.table")
        conn.execute('INSERT INTO "schema.table" VALUES (1, \'test\')')
        rows = get_audit_rows(conn, "schema.table")
        assert len(rows) == 1

    def test_table_with_quotes_in_name(self, conn):
        """Table names with single quotes (edge case)."""
        conn.execute(
            'CREATE TABLE "it\'s a table" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        enable_tracking(conn, "it's a table")
        conn.execute('INSERT INTO "it\'s a table" VALUES (1, \'hello\')')
        rows = get_audit_rows(conn, "it's a table")
        assert len(rows) == 1

    def test_rapid_sequence_of_operations(self, simple_table):
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'A', 1.0, 1)"
        )
        conn.execute("UPDATE items SET name = 'B' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'C' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'D' WHERE id = 1")
        rows = get_audit_rows(conn, "items")
        assert len(rows) == 4  # insert + 3 updates
        # Latest values
        vals = json.loads(rows[3]["updated_values"])
        assert vals["name"] == "D"

    def test_insert_update_delete_cycle(self, simple_table):
        """Full lifecycle: insert, update, delete, restore to each point."""
        conn = simple_table
        enable_tracking(conn, "items")

        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        rows_after_insert = get_audit_rows(conn, "items")
        id_insert = rows_after_insert[-1]["id"]

        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        rows_after_update = get_audit_rows(conn, "items")
        id_update = rows_after_update[-1]["id"]

        conn.execute("DELETE FROM items WHERE id = 1")

        # Restore after insert: should have Widget
        r1 = restore(conn, "items", up_to_id=id_insert, new_table_name="r1")
        row = conn.execute("SELECT * FROM r1 WHERE id = 1").fetchone()
        assert dict(row)["name"] == "Widget"

        # Restore after update: should have Gizmo
        r2 = restore(conn, "items", up_to_id=id_update, new_table_name="r2")
        row = conn.execute("SELECT * FROM r2 WHERE id = 1").fetchone()
        assert dict(row)["name"] == "Gizmo"

        # Restore after delete: should be empty
        r3 = restore(
            conn, "items", timestamp="9999-12-31 23:59:59", new_table_name="r3"
        )
        rows = conn.execute("SELECT * FROM r3").fetchall()
        assert len(rows) == 0

    def test_re_insert_after_delete(self, simple_table):
        """A row can be deleted and then a new row with the same PK inserted."""
        conn = simple_table
        enable_tracking(conn, "items")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("DELETE FROM items WHERE id = 1")
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'NewWidget', 5.99, 50)"
        )
        result_name = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        row = conn.execute(
            f"SELECT * FROM [{result_name}] WHERE id = 1"
        ).fetchone()
        assert dict(row)["name"] == "NewWidget"
        assert dict(row)["price"] == 5.99


# ---------------------------------------------------------------------------
# Tests: change grouping
# ---------------------------------------------------------------------------


class TestChangeGrouping:
    def test_groups_table_created_by_enable_tracking(self, simple_table):
        """enable_tracking should create the _history_json groups table."""
        enable_tracking(simple_table, "items")
        assert table_exists(simple_table, "_history_json")

    def test_audit_table_has_group_column(self, simple_table):
        """The audit table should have a [group] column."""
        enable_tracking(simple_table, "items")
        info = simple_table.execute(
            "PRAGMA table_info(_history_json_items)"
        ).fetchall()
        col_names = [r[1] for r in info]
        assert "group" in col_names

    def test_group_is_null_without_context_manager(self, simple_table):
        """Without change_group, audit rows should have group = NULL."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        rows = get_audit_rows(simple_table, "items")
        assert rows[0]["group"] is None

    def test_change_group_assigns_same_group(self, simple_table):
        """All changes within change_group should share the same group id."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table) as group_id:
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
            )
            simple_table.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        assert len(rows) == 3
        for row in rows:
            assert row["group"] == group_id

    def test_change_group_with_note(self, simple_table):
        """A note can be attached to a change group."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="bulk import"):
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
        # Check the groups table for the note
        group_row = simple_table.execute(
            "SELECT * FROM _history_json ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert dict(group_row)["note"] == "bulk import"

    def test_change_group_clears_current_after_exit(self, simple_table):
        """After the context manager exits, current should be NULL."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="test"):
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
        # No row should have current = 1
        current_rows = simple_table.execute(
            "SELECT * FROM _history_json WHERE current = 1"
        ).fetchall()
        assert len(current_rows) == 0

    def test_changes_after_group_have_null_group(self, simple_table):
        """Changes made after the context manager exits should have group = NULL."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table):
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
        # This insert is outside the group
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
        )
        rows = get_audit_rows(simple_table, "items")
        assert rows[0]["group"] is not None  # inside group
        assert rows[1]["group"] is None  # outside group

    def test_multiple_groups_are_distinct(self, simple_table):
        """Two separate change_group blocks should produce different group ids."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="first") as gid1:
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'A', 1.0, 1)"
            )
        with change_group(simple_table, note="second") as gid2:
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (2, 'B', 2.0, 2)"
            )
        assert gid1 != gid2
        rows = get_audit_rows(simple_table, "items")
        assert rows[0]["group"] == gid1
        assert rows[1]["group"] == gid2

    def test_change_group_with_delete(self, simple_table):
        """Delete operations should also be grouped."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        with change_group(simple_table, note="cleanup") as group_id:
            simple_table.execute("DELETE FROM items WHERE id = 1")
        rows = get_audit_rows(simple_table, "items")
        assert rows[0]["group"] is None  # the insert, outside group
        assert rows[1]["group"] == group_id  # the delete, inside group

    def test_change_group_yields_group_id(self, simple_table):
        """The context manager should yield an integer group id."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table) as group_id:
            assert isinstance(group_id, int)

    def test_change_group_note_can_be_updated(self, simple_table):
        """The note on the current group can be updated during the block."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="initial") as group_id:
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
            # Update the note mid-transaction
            simple_table.execute(
                "UPDATE _history_json SET note = 'updated' WHERE id = ?",
                [group_id],
            )
        group_row = simple_table.execute(
            "SELECT note FROM _history_json WHERE id = ?", [group_id]
        ).fetchone()
        assert dict(group_row)["note"] == "updated"

    def test_get_history_includes_group_info(self, simple_table):
        """get_history should include group and group_note in results."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="batch"):
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
        entries = get_history(simple_table, "items")
        assert len(entries) == 1
        assert "group" in entries[0]
        assert entries[0]["group"] is not None
        assert "group_note" in entries[0]
        assert entries[0]["group_note"] == "batch"

    def test_get_history_null_group(self, simple_table):
        """get_history should return None for group/group_note when no group."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        entries = get_history(simple_table, "items")
        assert entries[0]["group"] is None
        assert entries[0]["group_note"] is None

    def test_get_row_history_includes_group_info(self, simple_table):
        """get_row_history should include group info."""
        enable_tracking(simple_table, "items")
        with change_group(simple_table, note="row-note"):
            simple_table.execute(
                "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
            )
        entries = get_row_history(simple_table, "items", {"id": 1})
        assert entries[0]["group_note"] == "row-note"

    def test_change_group_across_multiple_tables(self, conn):
        """A single change_group should group changes across different tracked tables."""
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)"
        )
        enable_tracking(conn, "items")
        enable_tracking(conn, "orders")
        with change_group(conn, note="cross-table") as group_id:
            conn.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
            conn.execute("INSERT INTO orders (id, item_id) VALUES (1, 1)")
        item_rows = get_audit_rows(conn, "items")
        order_rows = get_audit_rows(conn, "orders")
        assert item_rows[0]["group"] == group_id
        assert order_rows[0]["group"] == group_id

    def test_populate_respects_change_group(self, simple_table_with_data):
        """populate() called within change_group should tag entries with that group."""
        conn = simple_table_with_data
        enable_tracking(conn, "items", populate_table=False)
        with change_group(conn, note="initial snapshot") as group_id:
            populate(conn, "items")
        rows = get_audit_rows(conn, "items")
        assert len(rows) == 3
        for row in rows:
            assert row["group"] == group_id

    def test_only_one_current_row_allowed(self, simple_table):
        """A unique partial index should prevent multiple current=1 rows."""
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO _history_json (note, current) VALUES ('first', 1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            simple_table.execute(
                "INSERT INTO _history_json (note, current) VALUES ('second', 1)"
            )
