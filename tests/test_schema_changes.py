"""Tests for schema changes (column add/remove) with triggers recreated but history rows left intact.

These tests explore how well the system handles columns being added to or
removed from a tracked table when:
1. The triggers are dropped and recreated after each schema change
2. The existing audit/history rows are left untouched

The workflow for each schema change is:
    disable_tracking(conn, table)       # drops triggers
    ALTER TABLE ... ADD/DROP COLUMN     # modify schema
    enable_tracking(conn, table, populate_table=False)  # recreate triggers
"""

import json
import sqlite3

import pytest

from sqlite_history_json import (
    disable_tracking,
    enable_tracking,
    get_history,
    get_row_history,
    restore,
    row_state_sql,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _audit_rows(conn, table_name):
    """Return all audit rows as dicts, ordered by id."""
    rows = conn.execute(
        f"SELECT * FROM [_history_json_{table_name}] ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _latest_audit_row(conn, table_name):
    """Return the most recent audit row as a dict."""
    row = conn.execute(
        f"SELECT * FROM [_history_json_{table_name}] ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    yield db
    db.close()


# ===========================================================================
# Column Addition
# ===========================================================================


class TestAddColumn:
    """Adding a column to a tracked table, then recreating triggers."""

    def _setup(self, conn):
        """Create table(id, name, price), enable tracking, insert+update some rows,
        then add a 'quantity' column and recreate triggers."""
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")
        conn.execute("UPDATE items SET price = 12.99 WHERE id = 1")
        conn.execute("INSERT INTO items VALUES (2, 'Gadget', 24.99)")

        # Schema change: add column
        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items ADD COLUMN quantity INTEGER")
        enable_tracking(conn, "items", populate_table=False)

    # -- History preservation --

    def test_old_history_rows_preserved(self, conn):
        """Existing history rows are untouched; they don't mention the new column."""
        self._setup(conn)
        rows = _audit_rows(conn, "items")
        assert len(rows) == 3  # insert(1), update(1), insert(2)

        vals_insert1 = json.loads(rows[0]["updated_values"])
        assert "quantity" not in vals_insert1
        assert vals_insert1 == {"name": "Widget", "price": 9.99}

        vals_update1 = json.loads(rows[1]["updated_values"])
        assert "quantity" not in vals_update1
        assert vals_update1 == {"price": 12.99}

        vals_insert2 = json.loads(rows[2]["updated_values"])
        assert "quantity" not in vals_insert2

    # -- New triggers work correctly --

    def test_new_insert_records_new_column(self, conn):
        """New inserts after schema change include the new column."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'Doohickey', 4.99, 50)")

        latest = json.loads(_latest_audit_row(conn, "items")["updated_values"])
        assert latest["name"] == "Doohickey"
        assert latest["price"] == 4.99
        assert latest["quantity"] == 50

    def test_new_insert_null_new_column(self, conn):
        """New inserts where the new column is NULL use the {null: 1} convention."""
        self._setup(conn)
        conn.execute("INSERT INTO items (id, name, price) VALUES (3, 'Thing', 1.99)")

        latest = json.loads(_latest_audit_row(conn, "items")["updated_values"])
        assert latest["quantity"] == {"null": 1}

    def test_update_new_column_recorded(self, conn):
        """Updating the new column on an existing row records just that change."""
        self._setup(conn)
        conn.execute("UPDATE items SET quantity = 100 WHERE id = 1")

        latest = json.loads(_latest_audit_row(conn, "items")["updated_values"])
        assert latest == {"quantity": 100}

    # -- get_history / get_row_history --

    def test_get_history_mixed_entries(self, conn):
        """get_history returns old entries (no new column) and new entries (with it)."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'New', 5.0, 75)")

        entries = get_history(conn, "items")  # newest first
        newest = entries[0]
        assert "quantity" in newest["updated_values"]
        assert newest["updated_values"]["quantity"] == 75

        oldest = entries[-1]
        assert "quantity" not in oldest["updated_values"]

    def test_get_row_history_spans_schema_change(self, conn):
        """get_row_history for a row that was modified before and after the column add."""
        self._setup(conn)
        conn.execute("UPDATE items SET quantity = 100 WHERE id = 1")

        entries = get_row_history(conn, "items", {"id": 1})
        # newest first: update(quantity), update(price), insert
        assert len(entries) == 3
        assert "quantity" in entries[0]["updated_values"]
        assert "quantity" not in entries[1]["updated_values"]
        assert "quantity" not in entries[2]["updated_values"]

    # -- restore --

    def test_restore_old_rows_get_null_for_new_column(self, conn):
        """Old insert entries that lack the new column result in NULL on restore."""
        self._setup(conn)

        result = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()

        row1 = dict(rows[0])
        assert row1["name"] == "Widget"
        assert row1["price"] == 12.99  # after the update
        assert row1["quantity"] is None  # column didn't exist in old history

        row2 = dict(rows[1])
        assert row2["name"] == "Gadget"
        assert row2["quantity"] is None

    def test_restore_mixed_old_and_new_entries(self, conn):
        """Restore handles a mix of pre- and post-schema-change entries."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'New', 5.0, 75)")
        conn.execute("UPDATE items SET quantity = 200 WHERE id = 1")

        result = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()

        assert len(rows) == 3
        assert dict(rows[0])["quantity"] == 200  # updated after column added
        assert dict(rows[1])["quantity"] is None  # never given a quantity
        assert dict(rows[2])["quantity"] == 75  # inserted with quantity

    def test_restore_to_point_before_column_added(self, conn):
        """Restoring to a point before the column was added produces a table
        with the current schema (including new column) but NULLs for it."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'New', 5.0, 75)")

        # The 3rd audit entry is the last one before schema change
        rows = _audit_rows(conn, "items")
        before_add_id = rows[2]["id"]

        result = restore(conn, "items", up_to_id=before_add_id)
        restored = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()

        assert len(restored) == 2
        for row in restored:
            assert dict(row)["quantity"] is None

    # -- row_state_sql --

    def test_row_state_sql_after_column_added(self, conn):
        """row_state_sql works when the row has been modified after the column add."""
        self._setup(conn)
        conn.execute("UPDATE items SET quantity = 100 WHERE id = 1")

        sql = row_state_sql(conn, "items")
        latest_id = conn.execute(
            "SELECT id FROM _history_json_items WHERE pk_id = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]

        result = conn.execute(sql, {"pk": 1, "target_id": latest_id}).fetchone()
        state = json.loads(result[0])
        assert state["name"] == "Widget"
        assert state["price"] == 12.99
        assert state["quantity"] == 100

    def test_row_state_sql_before_column_added(self, conn):
        """row_state_sql for a point before the column existed returns JSON
        without the new column key (it was never recorded)."""
        self._setup(conn)

        # Target the original insert entry for row 1
        insert_id = conn.execute(
            "SELECT id FROM _history_json_items WHERE pk_id = 1 ORDER BY id LIMIT 1"
        ).fetchone()[0]

        sql = row_state_sql(conn, "items")
        result = conn.execute(sql, {"pk": 1, "target_id": insert_id}).fetchone()
        state = json.loads(result[0])
        assert state["name"] == "Widget"
        assert state["price"] == 9.99
        assert "quantity" not in state  # column didn't exist then


# ===========================================================================
# Column Addition with DEFAULT
# ===========================================================================


class TestAddColumnWithDefault:
    """Adding a column with a DEFAULT value: existing source table rows
    get the default, but old history entries don't mention the column."""

    def test_restore_uses_null_not_default(self, conn):
        """Restore produces NULL for old rows, not the DEFAULT value,
        because the history never recorded the column."""
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget')")

        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items ADD COLUMN active INTEGER DEFAULT 1")
        enable_tracking(conn, "items", populate_table=False)

        # The live table has active=1 for row 1 (DEFAULT applied by SQLite)
        live = conn.execute("SELECT active FROM items WHERE id = 1").fetchone()
        assert live[0] == 1

        # But restore replays from history, which never recorded 'active'
        result = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        restored = conn.execute(
            f"SELECT active FROM [{result}] WHERE id = 1"
        ).fetchone()
        assert restored[0] is None  # NULL, not 1


# ===========================================================================
# Column Removal
# ===========================================================================


class TestRemoveColumn:
    """Removing a column from a tracked table, then recreating triggers."""

    def _setup(self, conn):
        """Create table(id, name, price, quantity), enable tracking,
        insert some rows (update only 'price'), then drop 'quantity' and recreate triggers."""
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY, name TEXT, price REAL, quantity INTEGER)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")
        conn.execute("UPDATE items SET price = 12.99 WHERE id = 1")
        conn.execute("INSERT INTO items VALUES (2, 'Gadget', 24.99, 50)")

        # Schema change: drop column
        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items DROP COLUMN quantity")
        enable_tracking(conn, "items", populate_table=False)

    # -- History preservation --

    def test_old_history_still_has_removed_column(self, conn):
        """Old audit rows retain the removed column in their JSON."""
        self._setup(conn)
        rows = _audit_rows(conn, "items")

        vals_insert1 = json.loads(rows[0]["updated_values"])
        assert vals_insert1["quantity"] == 100
        assert vals_insert1["name"] == "Widget"

        vals_insert2 = json.loads(rows[2]["updated_values"])
        assert vals_insert2["quantity"] == 50

    # -- New triggers --

    def test_new_inserts_omit_removed_column(self, conn):
        """New inserts don't record the removed column."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'Doohickey', 4.99)")

        latest = json.loads(_latest_audit_row(conn, "items")["updated_values"])
        assert "quantity" not in latest
        assert latest["name"] == "Doohickey"
        assert latest["price"] == 4.99

    def test_new_updates_omit_removed_column(self, conn):
        """New updates only reference columns that still exist."""
        self._setup(conn)
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")

        latest = json.loads(_latest_audit_row(conn, "items")["updated_values"])
        assert latest == {"name": "Gizmo"}
        assert "quantity" not in latest

    # -- get_history --

    def test_get_history_shows_mixed_entries(self, conn):
        """get_history returns all entries; old ones still have the removed column."""
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'New', 5.0)")

        entries = get_history(conn, "items")  # newest first
        newest = entries[0]
        assert "quantity" not in newest["updated_values"]

        oldest = entries[-1]
        assert "quantity" in oldest["updated_values"]
        assert oldest["updated_values"]["quantity"] == 100

    def test_get_row_history_spans_column_removal(self, conn):
        """get_row_history for a row modified before and after column removal."""
        self._setup(conn)
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")

        entries = get_row_history(conn, "items", {"id": 1})
        # newest first: update(name=Gizmo), update(price), insert
        assert len(entries) == 3
        # Old insert has the removed column
        assert "quantity" in entries[-1]["updated_values"]
        # New update does not
        assert "quantity" not in entries[0]["updated_values"]

    # -- restore --

    def test_restore_old_inserts_ignore_removed_column(self, conn):
        """Restore handles old insert entries that mention the removed column.

        The restore code iterates the *current* table schema for inserts,
        so extra keys in the JSON are silently ignored.
        """
        self._setup(conn)
        conn.execute("INSERT INTO items VALUES (3, 'New', 5.0)")

        result = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()

        assert len(rows) == 3
        row1 = dict(rows[0])
        assert row1["name"] == "Widget"
        assert row1["price"] == 12.99
        assert "quantity" not in row1  # column no longer exists

    def test_restore_fails_on_old_update_referencing_removed_column(self, conn):
        """Restore FAILS when replaying an old update that changed the now-removed column.

        The update replay iterates updated_values.items() and tries to
        SET each key as a column. If the column no longer exists in the
        target table, SQLite raises OperationalError.
        """
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY, name TEXT, price REAL, quantity INTEGER)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")
        # This update changes 'quantity' — which will later be removed
        conn.execute("UPDATE items SET quantity = 200 WHERE id = 1")

        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items DROP COLUMN quantity")
        enable_tracking(conn, "items", populate_table=False)

        with pytest.raises(sqlite3.OperationalError):
            restore(conn, "items", timestamp="9999-12-31 23:59:59")

    def test_restore_ok_when_old_updates_dont_touch_removed_column(self, conn):
        """Restore succeeds if old updates only modified still-existing columns."""
        self._setup(conn)
        # The update in _setup only changed 'price', not 'quantity'

        result = restore(conn, "items", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()
        assert len(rows) == 2
        assert dict(rows[0])["price"] == 12.99

    # -- row_state_sql --

    def test_row_state_sql_old_entries_include_removed_column(self, conn):
        """row_state_sql returns JSON that still includes the removed column,
        since the JSON was never modified."""
        self._setup(conn)

        sql = row_state_sql(conn, "items")
        latest_id = conn.execute(
            "SELECT id FROM _history_json_items WHERE pk_id = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]

        result = conn.execute(sql, {"pk": 1, "target_id": latest_id}).fetchone()
        state = json.loads(result[0])
        # The JSON state includes the removed column — it's just data
        assert state["quantity"] == 100
        assert state["price"] == 12.99


# ===========================================================================
# Multiple Schema Changes
# ===========================================================================


class TestMultipleSchemaChanges:
    """Multiple rounds of schema changes on the same tracked table."""

    def test_add_two_columns_in_stages(self, conn):
        """Add columns one at a time, each time recreating triggers."""
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)")
        enable_tracking(conn, "t")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")

        # Round 1: add column b
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN b INTEGER")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("INSERT INTO t VALUES (2, 'world', 42)")
        conn.execute("UPDATE t SET b = 10 WHERE id = 1")

        # Round 2: add column c
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN c REAL")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("INSERT INTO t VALUES (3, 'foo', 99, 3.14)")

        result = restore(conn, "t", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()

        assert len(rows) == 3
        r1 = dict(rows[0])
        assert r1["a"] == "hello"
        assert r1["b"] == 10
        assert r1["c"] is None  # never set

        r2 = dict(rows[1])
        assert r2["b"] == 42
        assert r2["c"] is None  # not in any entry

        r3 = dict(rows[2])
        assert r3["b"] == 99
        assert r3["c"] == 3.14

    def test_add_then_remove_column(self, conn):
        """Add a column, use it, then remove it. History has a mix of entries."""
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        enable_tracking(conn, "t")
        conn.execute("INSERT INTO t VALUES (1, 'original')")

        # Add column 'extra'
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("UPDATE t SET extra = 'bonus' WHERE id = 1")
        conn.execute("INSERT INTO t VALUES (2, 'second', 'has-extra')")

        # Remove column 'extra'
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t DROP COLUMN extra")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("INSERT INTO t VALUES (3, 'third')")

        # History should have entries from all three eras
        entries = get_history(conn, "t")  # newest first
        # Newest: insert(3) — no 'extra'
        assert "extra" not in entries[0]["updated_values"]
        # Middle: insert(2) — has 'extra'
        assert entries[1]["updated_values"]["extra"] == "has-extra"
        # Update era entry: has 'extra'
        assert entries[2]["updated_values"]["extra"] == "bonus"
        # Oldest: insert(1) — no 'extra' (added later)
        assert "extra" not in entries[3]["updated_values"]

    def test_add_then_remove_column_restore_fails_if_update_touched_removed(self, conn):
        """After add-then-remove, restore fails because an old update references
        the column that was subsequently removed."""
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        enable_tracking(conn, "t")
        conn.execute("INSERT INTO t VALUES (1, 'original')")

        # Add column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("UPDATE t SET extra = 'bonus' WHERE id = 1")

        # Remove column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t DROP COLUMN extra")
        enable_tracking(conn, "t", populate_table=False)

        # Restore will fail on the update entry {'extra': 'bonus'}
        with pytest.raises(sqlite3.OperationalError):
            restore(conn, "t", timestamp="9999-12-31 23:59:59")

    def test_add_then_remove_column_restore_works_if_no_update_on_removed(self, conn):
        """After add-then-remove, restore succeeds if no update touched the removed column."""
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        enable_tracking(conn, "t")
        conn.execute("INSERT INTO t VALUES (1, 'original')")

        # Add column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        enable_tracking(conn, "t", populate_table=False)
        # Only insert with the extra column, no updates to 'extra' specifically
        conn.execute("INSERT INTO t VALUES (2, 'second', 'has-extra')")

        # Remove column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t DROP COLUMN extra")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("INSERT INTO t VALUES (3, 'third')")

        # Restore succeeds because insert replay ignores extra JSON keys
        result = restore(conn, "t", timestamp="9999-12-31 23:59:59")
        rows = conn.execute(f"SELECT * FROM [{result}] ORDER BY id").fetchall()
        assert len(rows) == 3
        assert dict(rows[0])["name"] == "original"
        assert dict(rows[1])["name"] == "second"
        assert dict(rows[2])["name"] == "third"

    def test_row_state_sql_across_add_and_remove(self, conn):
        """row_state_sql returns JSON that reflects whatever was recorded,
        including keys for columns that have since been removed."""
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        enable_tracking(conn, "t")
        conn.execute("INSERT INTO t VALUES (1, 'original')")

        # Add column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
        enable_tracking(conn, "t", populate_table=False)
        conn.execute("UPDATE t SET extra = 'bonus' WHERE id = 1")

        # Remove column
        disable_tracking(conn, "t")
        conn.execute("ALTER TABLE t DROP COLUMN extra")
        enable_tracking(conn, "t", populate_table=False)

        sql = row_state_sql(conn, "t")
        # Get the latest audit entry for row 1 (the update that set extra)
        latest_id = conn.execute(
            "SELECT id FROM _history_json_t WHERE pk_id = 1 ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]

        result = conn.execute(sql, {"pk": 1, "target_id": latest_id}).fetchone()
        state = json.loads(result[0])
        # JSON still has the removed column — it's just data in the audit log
        assert state["name"] == "original"
        assert state["extra"] == "bonus"


# ===========================================================================
# Re-enabling with populate after schema change
# ===========================================================================


class TestRepopulateAfterSchemaChange:
    """What happens if you repopulate (snapshot) after a schema change."""

    def test_repopulate_after_add_column_creates_complete_snapshot(self, conn):
        """Calling populate after adding a column creates new insert entries
        that include the new column's current value from the live table."""
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget')")

        # Add column with default
        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items ADD COLUMN score INTEGER DEFAULT 0")
        # Drop old audit entries and re-enable with fresh populate
        conn.execute("DELETE FROM _history_json_items")
        enable_tracking(conn, "items", populate_table=True)

        rows = _audit_rows(conn, "items")
        assert len(rows) == 1
        vals = json.loads(rows[0]["updated_values"])
        # Populate reads the current live row, which has score=0 from DEFAULT
        assert vals["name"] == "Widget"
        assert vals["score"] == 0

    def test_repopulate_after_remove_column_creates_clean_snapshot(self, conn):
        """Repopulating after removing a column creates entries without the old column."""
        conn.execute(
            "CREATE TABLE items ("
            "id INTEGER PRIMARY KEY, name TEXT, quantity INTEGER)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget', 100)")

        # Remove column
        disable_tracking(conn, "items")
        conn.execute("ALTER TABLE items DROP COLUMN quantity")
        # Drop old audit entries and re-enable with fresh populate
        conn.execute("DELETE FROM _history_json_items")
        enable_tracking(conn, "items", populate_table=True)

        rows = _audit_rows(conn, "items")
        assert len(rows) == 1
        vals = json.loads(rows[0]["updated_values"])
        assert vals == {"name": "Widget"}
        assert "quantity" not in vals
