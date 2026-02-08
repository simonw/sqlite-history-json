"""Tests for row_state_sql() — generates SQL to reconstruct row state at a version."""

import json
import sqlite3

import pytest

from sqlite_history_json import enable_tracking, row_state_sql


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    yield db
    db.close()


@pytest.fixture
def simple_table(conn):
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
def compound_pk_table(conn):
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


class TestRowStateSqlErrors:
    def test_error_if_tracking_not_enabled(self, simple_table):
        with pytest.raises(ValueError, match="not enabled"):
            row_state_sql(simple_table, "items")

    def test_error_if_table_does_not_exist(self, conn):
        with pytest.raises(ValueError):
            row_state_sql(conn, "nonexistent")


class TestRowStateSqlSinglePk:
    def test_returns_string(self, simple_table):
        enable_tracking(simple_table, "items")
        sql = row_state_sql(simple_table, "items")
        assert isinstance(sql, str)

    def test_uses_pk_param(self, simple_table):
        enable_tracking(simple_table, "items")
        sql = row_state_sql(simple_table, "items")
        assert ":pk" in sql
        assert ":target_id" in sql

    def test_reconstruct_after_insert(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 1}).fetchone()
        state = json.loads(result[0])
        assert state == {"name": "Widget", "price": 9.99, "quantity": 100}

    def test_reconstruct_after_update(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute(
            "UPDATE items SET name = 'Super Widget', price = 12.99 WHERE id = 1"
        )
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 2}).fetchone()
        state = json.loads(result[0])
        assert state == {"name": "Super Widget", "price": 12.99, "quantity": 100}

    def test_reconstruct_at_earlier_version(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 1}).fetchone()
        state = json.loads(result[0])
        assert state["name"] == "Widget"

    def test_reconstruct_after_delete_returns_null(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("DELETE FROM items WHERE id = 1")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 2}).fetchone()
        assert result[0] is None

    def test_reconstruct_after_reinsert(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("DELETE FROM items WHERE id = 1")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'New Widget', 5.99, 50)"
        )
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 3}).fetchone()
        state = json.loads(result[0])
        assert state["name"] == "New Widget"
        assert state["price"] == 5.99

    def test_null_value_convention(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 1}).fetchone()
        state = json.loads(result[0])
        assert state["price"] == {"null": 1}
        assert state["quantity"] == {"null": 1}

    def test_update_to_null(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("UPDATE items SET price = NULL WHERE id = 1")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 2}).fetchone()
        state = json.loads(result[0])
        assert state["price"] == {"null": 1}

    def test_update_from_null(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")
        simple_table.execute("UPDATE items SET price = 5.99 WHERE id = 1")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 2}).fetchone()
        state = json.loads(result[0])
        assert state["price"] == 5.99

    def test_no_result_for_nonexistent_row(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 999, "target_id": 1}).fetchone()
        assert result is None

    def test_multiple_updates_folded(self, simple_table):
        enable_tracking(simple_table, "items")
        simple_table.execute(
            "INSERT INTO items VALUES (1, 'Widget', 9.99, 100)"
        )
        simple_table.execute("UPDATE items SET name = 'A' WHERE id = 1")
        simple_table.execute("UPDATE items SET price = 1.99 WHERE id = 1")
        simple_table.execute("UPDATE items SET quantity = 5 WHERE id = 1")
        sql = row_state_sql(simple_table, "items")
        result = simple_table.execute(sql, {"pk": 1, "target_id": 4}).fetchone()
        state = json.loads(result[0])
        assert state == {"name": "A", "price": 1.99, "quantity": 5}


class TestRowStateSqlCompoundPk:
    def test_uses_numbered_pk_params(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        sql = row_state_sql(compound_pk_table, "user_roles")
        assert ":pk_1" in sql
        assert ":pk_2" in sql
        assert ":target_id" in sql

    def test_reconstruct_compound_pk(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 3, 'system', 1)"
        )
        sql = row_state_sql(compound_pk_table, "user_roles")
        result = compound_pk_table.execute(
            sql, {"pk_1": 1, "pk_2": 2, "target_id": 1}
        ).fetchone()
        state = json.loads(result[0])
        assert state == {"granted_by": "admin", "active": 1}

    def test_compound_pk_update(self, compound_pk_table):
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        compound_pk_table.execute(
            "UPDATE user_roles SET active = 0 WHERE user_id = 1 AND role_id = 2"
        )
        sql = row_state_sql(compound_pk_table, "user_roles")
        result = compound_pk_table.execute(
            sql, {"pk_1": 1, "pk_2": 2, "target_id": 2}
        ).fetchone()
        state = json.loads(result[0])
        assert state == {"granted_by": "admin", "active": 0}

    def test_compound_pk_filters_correctly(self, compound_pk_table):
        """Ensure (1,2) and (1,3) don't mix."""
        enable_tracking(compound_pk_table, "user_roles")
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        compound_pk_table.execute(
            "INSERT INTO user_roles VALUES (1, 3, 'system', 0)"
        )
        sql = row_state_sql(compound_pk_table, "user_roles")
        result = compound_pk_table.execute(
            sql, {"pk_1": 1, "pk_2": 3, "target_id": 2}
        ).fetchone()
        state = json.loads(result[0])
        assert state == {"granted_by": "system", "active": 0}
