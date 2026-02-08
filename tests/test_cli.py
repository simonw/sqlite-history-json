"""Tests for the sqlite-history-json CLI."""

import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


def run_cli(*args: str, input_text: str | None = None) -> subprocess.CompletedProcess:
    """Run the CLI via subprocess and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "sqlite_history_json", *args],
        capture_output=True,
        text=True,
        input=input_text,
    )


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database with a simple table and return its path."""
    path = str(tmp_path / "test.db")
    conn = sqlite3.connect(path)
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
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def db_path_with_data(db_path):
    """Database with some rows already inserted."""
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO items (id, name, price, quantity) VALUES (?, ?, ?, ?)",
        [
            (1, "Widget", 9.99, 100),
            (2, "Gadget", 24.99, 50),
            (3, "Doohickey", 4.99, 200),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def compound_pk_db(tmp_path):
    """Database with a compound primary key table."""
    path = str(tmp_path / "compound.db")
    conn = sqlite3.connect(path)
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
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# Tests: --help
# ---------------------------------------------------------------------------


class TestHelp:
    def test_help(self):
        result = run_cli("--help")
        assert result.returncode == 0
        assert "sqlite_history_json" in result.stdout

    def test_enable_help(self, db_path):
        result = run_cli(db_path, "enable", "--help")
        assert result.returncode == 0
        assert "--no-populate" in result.stdout


# ---------------------------------------------------------------------------
# Tests: enable command
# ---------------------------------------------------------------------------


class TestEnableCommand:
    def test_enable_creates_audit_table(self, db_path):
        result = run_cli(db_path, "enable", "items")
        assert result.returncode == 0
        assert "Tracking enabled" in result.stderr

        conn = sqlite3.connect(db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_history_json_%'"
        ).fetchall()
        conn.close()
        assert len(tables) == 1
        assert tables[0][0] == "_history_json_items"

    def test_enable_populates_existing_rows(self, db_path_with_data):
        result = run_cli(db_path_with_data, "enable", "items")
        assert result.returncode == 0

        conn = sqlite3.connect(db_path_with_data)
        count = conn.execute(
            "SELECT count(*) FROM _history_json_items"
        ).fetchone()[0]
        conn.close()
        assert count == 3

    def test_enable_no_populate(self, db_path_with_data):
        result = run_cli(db_path_with_data, "enable", "items", "--no-populate")
        assert result.returncode == 0

        conn = sqlite3.connect(db_path_with_data)
        count = conn.execute(
            "SELECT count(*) FROM _history_json_items"
        ).fetchone()[0]
        conn.close()
        assert count == 0

    def test_enable_idempotent(self, db_path):
        run_cli(db_path, "enable", "items")
        result = run_cli(db_path, "enable", "items")
        assert result.returncode == 0


# ---------------------------------------------------------------------------
# Tests: disable command
# ---------------------------------------------------------------------------


class TestDisableCommand:
    def test_disable_removes_triggers(self, db_path):
        run_cli(db_path, "enable", "items")
        result = run_cli(db_path, "disable", "items")
        assert result.returncode == 0
        assert "Tracking disabled" in result.stderr

        conn = sqlite3.connect(db_path)
        triggers = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='items'"
        ).fetchall()
        conn.close()
        assert len(triggers) == 0

    def test_disable_keeps_audit_table(self, db_path):
        run_cli(db_path, "enable", "items")
        run_cli(db_path, "disable", "items")

        conn = sqlite3.connect(db_path)
        exists = conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_history_json_items'"
        ).fetchone()[0]
        conn.close()
        assert exists == 1


# ---------------------------------------------------------------------------
# Tests: history command
# ---------------------------------------------------------------------------


class TestHistoryCommand:
    def test_history_returns_json(self, db_path):
        run_cli(db_path, "enable", "items")

        # Insert a row directly
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.commit()
        conn.close()

        result = run_cli(db_path, "history", "items")
        assert result.returncode == 0
        entries = json.loads(result.stdout)
        assert len(entries) == 1
        assert entries[0]["operation"] == "insert"
        assert entries[0]["pk"] == {"id": 1}
        assert entries[0]["updated_values"]["name"] == "Widget"

    def test_history_newest_first(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(db_path, "history", "items")
        entries = json.loads(result.stdout)
        assert len(entries) == 2
        assert entries[0]["operation"] == "update"
        assert entries[1]["operation"] == "insert"

    def test_history_limit(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'Thingamajig' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(db_path, "history", "items", "-n", "2")
        entries = json.loads(result.stdout)
        assert len(entries) == 2

    def test_history_populated_data(self, db_path_with_data):
        run_cli(db_path_with_data, "enable", "items")

        result = run_cli(db_path_with_data, "history", "items")
        entries = json.loads(result.stdout)
        assert len(entries) == 3
        # All should be insert operations from population
        assert all(e["operation"] == "insert" for e in entries)

    def test_history_delete_has_null_updated_values(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("DELETE FROM items WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(db_path, "history", "items")
        entries = json.loads(result.stdout)
        delete_entry = entries[0]  # newest first
        assert delete_entry["operation"] == "delete"
        assert delete_entry["updated_values"] is None


# ---------------------------------------------------------------------------
# Tests: row-history command
# ---------------------------------------------------------------------------


class TestRowHistoryCommand:
    def test_row_history_single_pk(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(db_path, "row-history", "items", "1")
        assert result.returncode == 0
        entries = json.loads(result.stdout)
        assert len(entries) == 2  # insert + update for id=1 only
        assert all(e["pk"] == {"id": 1} for e in entries)

    def test_row_history_compound_pk(self, compound_pk_db):
        run_cli(compound_pk_db, "enable", "user_roles")

        conn = sqlite3.connect(compound_pk_db)
        conn.execute(
            "INSERT INTO user_roles VALUES (1, 2, 'admin', 1)"
        )
        conn.execute(
            "INSERT INTO user_roles VALUES (3, 4, 'system', 0)"
        )
        conn.execute(
            "UPDATE user_roles SET active = 0 WHERE user_id = 1 AND role_id = 2"
        )
        conn.commit()
        conn.close()

        result = run_cli(compound_pk_db, "row-history", "user_roles", "1", "2")
        assert result.returncode == 0
        entries = json.loads(result.stdout)
        assert len(entries) == 2  # insert + update
        assert all(e["pk"] == {"user_id": 1, "role_id": 2} for e in entries)

    def test_row_history_wrong_pk_count(self, db_path):
        run_cli(db_path, "enable", "items")
        result = run_cli(db_path, "row-history", "items", "1", "2")
        assert result.returncode == 1
        assert "1 primary key column(s)" in result.stderr

    def test_row_history_limit(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'A' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'B' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(db_path, "row-history", "items", "1", "-n", "2")
        entries = json.loads(result.stdout)
        assert len(entries) == 2


# ---------------------------------------------------------------------------
# Tests: restore command
# ---------------------------------------------------------------------------


class TestRestoreCommand:
    def test_restore_creates_new_table(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.commit()
        conn.close()

        result = run_cli(db_path, "restore", "items")
        assert result.returncode == 0
        assert "items_restored" in result.stderr

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM items_restored").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_restore_with_id(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        conn.commit()

        # Get the insert entry id
        audit_id = conn.execute(
            "SELECT id FROM _history_json_items ORDER BY id LIMIT 1"
        ).fetchone()[0]
        conn.close()

        result = run_cli(db_path, "restore", "items", "--id", str(audit_id))
        assert result.returncode == 0

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT name FROM items_restored WHERE id = 1").fetchone()
        conn.close()
        assert row[0] == "Widget"

    def test_restore_with_timestamp(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.commit()
        conn.close()

        result = run_cli(
            db_path, "restore", "items", "--timestamp", "9999-12-31 23:59:59"
        )
        assert result.returncode == 0

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM items_restored").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_restore_new_table_name(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.commit()
        conn.close()

        result = run_cli(db_path, "restore", "items", "--new-table", "items_v2")
        assert result.returncode == 0
        assert "items_v2" in result.stderr

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM items_v2").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_restore_replace_table(self, db_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        # Get audit id of the insert
        audit_id = conn.execute(
            "SELECT id FROM _history_json_items ORDER BY id LIMIT 1"
        ).fetchone()[0]
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_cli(
            db_path, "restore", "items", "--id", str(audit_id), "--replace-table"
        )
        assert result.returncode == 0
        assert "replaced" in result.stderr

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT name FROM items WHERE id = 1").fetchone()
        conn.close()
        assert row[0] == "Widget"

    def test_restore_output_db(self, db_path, tmp_path):
        run_cli(db_path, "enable", "items")

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)"
        )
        conn.execute(
            "INSERT INTO items (id, name, price, quantity) VALUES (2, 'Gadget', 24.99, 50)"
        )
        conn.commit()
        conn.close()

        output_db = str(tmp_path / "backup.db")
        result = run_cli(db_path, "restore", "items", "--output-db", output_db)
        assert result.returncode == 0
        assert "backup.db" in result.stderr

        conn = sqlite3.connect(output_db)
        rows = conn.execute("SELECT * FROM items ORDER BY id").fetchall()
        conn.close()
        assert len(rows) == 2

    def test_restore_replace_and_output_db_mutually_exclusive(self, db_path, tmp_path):
        run_cli(db_path, "enable", "items")
        output_db = str(tmp_path / "backup.db")
        result = run_cli(
            db_path,
            "restore",
            "items",
            "--replace-table",
            "--output-db",
            output_db,
        )
        assert result.returncode != 0


# ---------------------------------------------------------------------------
# Tests: get_history / get_row_history (core functions)
# ---------------------------------------------------------------------------


class TestGetHistory:
    def test_get_history_basic(self):
        from sqlite_history_json import enable_tracking, get_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")

        entries = get_history(conn, "items")
        assert len(entries) == 2
        # Newest first
        assert entries[0]["operation"] == "update"
        assert entries[1]["operation"] == "insert"
        assert entries[0]["pk"] == {"id": 1}
        assert entries[0]["updated_values"] == {"name": "Gizmo"}
        conn.close()

    def test_get_history_limit(self):
        from sqlite_history_json import enable_tracking, get_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'A')")
        conn.execute("UPDATE items SET name = 'B' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'C' WHERE id = 1")

        entries = get_history(conn, "items", limit=2)
        assert len(entries) == 2
        conn.close()

    def test_get_history_delete_has_none_updated_values(self):
        from sqlite_history_json import enable_tracking, get_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget')")
        conn.execute("DELETE FROM items WHERE id = 1")

        entries = get_history(conn, "items")
        delete_entry = entries[0]
        assert delete_entry["operation"] == "delete"
        assert delete_entry["updated_values"] is None
        conn.close()

    def test_get_history_preserves_null_convention(self):
        from sqlite_history_json import enable_tracking, get_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items (id, name) VALUES (1, 'Widget')")

        entries = get_history(conn, "items")
        assert entries[0]["updated_values"]["price"] == {"null": 1}
        conn.close()


class TestGetRowHistory:
    def test_get_row_history_filters_by_pk(self):
        from sqlite_history_json import enable_tracking, get_row_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'Widget')")
        conn.execute("INSERT INTO items VALUES (2, 'Gadget')")
        conn.execute("UPDATE items SET name = 'Gizmo' WHERE id = 1")

        entries = get_row_history(conn, "items", {"id": 1})
        assert len(entries) == 2
        assert all(e["pk"] == {"id": 1} for e in entries)
        conn.close()

    def test_get_row_history_compound_pk(self):
        from sqlite_history_json import enable_tracking, get_row_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE user_roles (
                user_id INTEGER,
                role_id INTEGER,
                granted_by TEXT,
                PRIMARY KEY (user_id, role_id)
            )
            """
        )
        enable_tracking(conn, "user_roles")
        conn.execute("INSERT INTO user_roles VALUES (1, 2, 'admin')")
        conn.execute("INSERT INTO user_roles VALUES (3, 4, 'system')")

        entries = get_row_history(conn, "user_roles", {"user_id": 1, "role_id": 2})
        assert len(entries) == 1
        assert entries[0]["pk"] == {"user_id": 1, "role_id": 2}
        conn.close()

    def test_get_row_history_limit(self):
        from sqlite_history_json import enable_tracking, get_row_history

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        enable_tracking(conn, "items")
        conn.execute("INSERT INTO items VALUES (1, 'A')")
        conn.execute("UPDATE items SET name = 'B' WHERE id = 1")
        conn.execute("UPDATE items SET name = 'C' WHERE id = 1")

        entries = get_row_history(conn, "items", {"id": 1}, limit=2)
        assert len(entries) == 2
        conn.close()
