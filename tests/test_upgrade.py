"""Tests for the sqlite_history_json.upgrade module."""

import sqlite3
import subprocess
import sys
import tempfile

import pytest

from sqlite_history_json import (
    change_group,
    enable_tracking,
    get_history,
    get_row_history,
)
from sqlite_history_json.core import _TRIGGER_VERSION
from sqlite_history_json.upgrade import (
    _find_audit_tables,
    _has_column,
    _trigger_needs_upgrade,
    apply_upgrade,
    detect_upgrades,
    main,
)


# ---------------------------------------------------------------------------
# Helpers for simulating old-style (pre-group) databases
# ---------------------------------------------------------------------------


def _create_old_style_tracking(conn: sqlite3.Connection, table_name: str) -> None:
    """Set up audit table and triggers the way they looked before change-grouping.

    This creates an audit table WITHOUT the [group] column and triggers
    that do NOT populate it — matching the schema from before commit a80c34d.
    """
    columns = conn.execute(f"pragma table_info([{table_name}])").fetchall()
    pk_cols = sorted(
        [c for c in columns if c[5] > 0], key=lambda c: c[5]
    )
    non_pk_cols = [c for c in columns if c[5] == 0]

    audit_name = f"_history_json_{table_name}"

    pk_col_defs = ", ".join(f"[pk_{c[1]}] {c[2]}" for c in pk_cols)

    # Old-style audit table: no [group] column
    conn.execute(
        f"""create table [{audit_name}] (
    id integer primary key,
    timestamp text,
    operation text,
    {pk_col_defs},
    updated_values text
)"""
    )

    # Old-style insert trigger: no [group]
    audit_pk_names = ", ".join(f"[pk_{c[1]}]" for c in pk_cols)
    pk_new_refs = ", ".join(f"NEW.[{c[1]}]" for c in pk_cols)
    json_args = ", ".join(
        f"'{c[1]}', case when NEW.[{c[1]}] is null "
        f"then json_object('null', 1) else NEW.[{c[1]}] end"
        for c in non_pk_cols
    )
    json_obj = f"json_object({json_args})" if json_args else "'{{}}'"

    conn.execute(
        f"""create trigger [{audit_name}_insert]
after insert on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values)
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'insert',
        {pk_new_refs},
        {json_obj}
    );
end;"""
    )

    # Old-style update trigger (simplified: records all non-PK cols)
    conn.execute(
        f"""create trigger [{audit_name}_update]
after update on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values)
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'update',
        {pk_new_refs},
        {json_obj}
    );
end;"""
    )

    # Old-style delete trigger
    pk_old_refs = ", ".join(f"OLD.[{c[1]}]" for c in pk_cols)
    conn.execute(
        f"""create trigger [{audit_name}_delete]
after delete on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values)
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'delete',
        {pk_old_refs},
        null
    );
end;"""
    )

    # Create indexes (same as current code)
    conn.execute(
        f"create index [{audit_name}_timestamp] on [{audit_name}] (timestamp)"
    )
    audit_pk_col_names = ", ".join(f"[pk_{c[1]}]" for c in pk_cols)
    conn.execute(
        f"create index [{audit_name}_pk] on [{audit_name}] ({audit_pk_col_names})"
    )


def _create_v1_style_tracking(conn: sqlite3.Connection, table_name: str) -> None:
    """Set up audit table and triggers with group column but old (unversioned) names.

    This simulates a database created after change-grouping was added but before
    trigger names were versioned. Triggers are named ``_history_json_{table}_{op}``
    (the old convention) but include the [group] column.
    """
    columns = conn.execute(f"pragma table_info([{table_name}])").fetchall()
    pk_cols = sorted(
        [c for c in columns if c[5] > 0], key=lambda c: c[5]
    )
    non_pk_cols = [c for c in columns if c[5] == 0]

    audit_name = f"_history_json_{table_name}"

    pk_col_defs = ", ".join(f"[pk_{c[1]}] {c[2]}" for c in pk_cols)

    # Create the groups table
    conn.execute(
        """create table if not exists [_history_json] (
    id integer primary key,
    note text,
    current integer
)"""
    )
    conn.execute(
        "create unique index if not exists [_history_json_current] "
        "on [_history_json] (current) where current = 1"
    )

    # Audit table WITH [group] column (v1 schema)
    conn.execute(
        f"""create table [{audit_name}] (
    id integer primary key,
    timestamp text,
    operation text,
    {pk_col_defs},
    updated_values text,
    [group] integer references [_history_json](id)
)"""
    )

    # v1-style triggers: include [group] but use old naming convention
    audit_pk_names = ", ".join(f"[pk_{c[1]}]" for c in pk_cols)
    pk_new_refs = ", ".join(f"NEW.[{c[1]}]" for c in pk_cols)
    group_sub = "(select id from [_history_json] where current = 1)"
    json_args = ", ".join(
        f"'{c[1]}', case when NEW.[{c[1]}] is null "
        f"then json_object('null', 1) else NEW.[{c[1]}] end"
        for c in non_pk_cols
    )
    json_obj = f"json_object({json_args})" if json_args else "'{{}}'"

    # Old naming: {audit_name}_{operation}
    conn.execute(
        f"""create trigger [{audit_name}_insert]
after insert on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'insert',
        {pk_new_refs},
        {json_obj},
        {group_sub}
    );
end;"""
    )

    conn.execute(
        f"""create trigger [{audit_name}_update]
after update on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'update',
        {pk_new_refs},
        {json_obj},
        {group_sub}
    );
end;"""
    )

    pk_old_refs = ", ".join(f"OLD.[{c[1]}]" for c in pk_cols)
    conn.execute(
        f"""create trigger [{audit_name}_delete]
after delete on [{table_name}]
begin
    insert into [{audit_name}] (timestamp, operation, {audit_pk_names}, updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'delete',
        {pk_old_refs},
        null,
        {group_sub}
    );
end;"""
    )

    conn.execute(
        f"create index [{audit_name}_timestamp] on [{audit_name}] (timestamp)"
    )
    audit_pk_col_names = ", ".join(f"[pk_{c[1]}]" for c in pk_cols)
    conn.execute(
        f"create index [{audit_name}_pk] on [{audit_name}] ({audit_pk_col_names})"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    yield db
    db.close()


@pytest.fixture
def old_db(conn):
    """Database with an old-style tracked table (no group column)."""
    conn.execute(
        "create table items ("
        "id integer primary key, name text, price float, quantity integer)"
    )
    _create_old_style_tracking(conn, "items")
    # Insert some data so there's history
    conn.execute(
        "insert into items (id, name, price, quantity) values (1, 'Widget', 9.99, 100)"
    )
    conn.execute("update items set price = 12.99 where id = 1")
    return conn


@pytest.fixture
def old_db_multi(conn):
    """Database with two old-style tracked tables."""
    conn.execute(
        "create table items ("
        "id integer primary key, name text, price float)"
    )
    conn.execute(
        "create table orders ("
        "id integer primary key, item_id integer, qty integer)"
    )
    _create_old_style_tracking(conn, "items")
    _create_old_style_tracking(conn, "orders")
    conn.execute("insert into items (id, name, price) values (1, 'Widget', 9.99)")
    conn.execute("insert into orders (id, item_id, qty) values (1, 1, 5)")
    return conn


@pytest.fixture
def v1_db(conn):
    """Database with v1-style tracked table (has group column, unversioned triggers)."""
    conn.execute(
        "create table items ("
        "id integer primary key, name text, price float, quantity integer)"
    )
    _create_v1_style_tracking(conn, "items")
    conn.execute(
        "insert into items (id, name, price, quantity) values (1, 'Widget', 9.99, 100)"
    )
    conn.execute("update items set price = 12.99 where id = 1")
    return conn


@pytest.fixture
def old_db_compound_pk(conn):
    """Database with an old-style tracked table using compound PK."""
    conn.execute(
        "create table user_roles ("
        "user_id integer, role_id integer, granted_by text, "
        "primary key (user_id, role_id))"
    )
    _create_old_style_tracking(conn, "user_roles")
    conn.execute(
        "insert into user_roles (user_id, role_id, granted_by) "
        "values (1, 10, 'admin')"
    )
    return conn


# ---------------------------------------------------------------------------
# Tests: detection
# ---------------------------------------------------------------------------


class TestDetectUpgrades:
    def test_detects_missing_group_column(self, old_db):
        actions = detect_upgrades(old_db)
        assert len(actions) == 1
        assert actions[0]["audit_table"] == "_history_json_items"
        assert actions[0]["source_table"] == "items"
        assert actions[0]["needs_column"] is True
        assert actions[0]["source_exists"] is True

    def test_detects_old_triggers(self, old_db):
        actions = detect_upgrades(old_db)
        assert actions[0]["needs_triggers"] is True

    def test_nothing_to_upgrade_on_current_schema(self, conn):
        """A database created with the current code needs no upgrades."""
        conn.execute(
            "create table items ("
            "id integer primary key, name text, price float)"
        )
        enable_tracking(conn, "items")
        actions = detect_upgrades(conn)
        assert actions == []

    def test_detects_multiple_tables(self, old_db_multi):
        actions = detect_upgrades(old_db_multi)
        audit_names = {a["audit_table"] for a in actions}
        assert audit_names == {"_history_json_items", "_history_json_orders"}
        assert all(a["needs_column"] for a in actions)
        assert all(a["needs_triggers"] for a in actions)

    def test_empty_database_nothing_to_upgrade(self, conn):
        actions = detect_upgrades(conn)
        assert actions == []

    def test_detects_compound_pk_table(self, old_db_compound_pk):
        actions = detect_upgrades(old_db_compound_pk)
        assert len(actions) == 1
        assert actions[0]["audit_table"] == "_history_json_user_roles"

    def test_source_table_gone(self, conn):
        """If the source table was dropped, still detect column issue."""
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        _create_old_style_tracking(conn, "items")
        conn.execute("drop trigger [_history_json_items_insert]")
        conn.execute("drop trigger [_history_json_items_update]")
        conn.execute("drop trigger [_history_json_items_delete]")
        conn.execute("drop table items")
        actions = detect_upgrades(conn)
        assert len(actions) == 1
        assert actions[0]["needs_column"] is True
        assert actions[0]["source_exists"] is False
        # Can't upgrade triggers without the source table
        assert actions[0]["needs_triggers"] is False

    def test_column_present_but_triggers_old(self, conn):
        """If column was manually added but triggers are old-style."""
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        _create_old_style_tracking(conn, "items")
        # Manually add the column
        conn.execute(
            "alter table [_history_json_items] "
            "add column [group] integer references [_history_json](id)"
        )
        actions = detect_upgrades(conn)
        assert len(actions) == 1
        assert actions[0]["needs_column"] is False
        assert actions[0]["needs_triggers"] is True

    def test_detects_v1_unversioned_triggers(self, v1_db):
        """v1-style triggers (with group, but unversioned names) need upgrade."""
        actions = detect_upgrades(v1_db)
        assert len(actions) == 1
        assert actions[0]["needs_column"] is False
        assert actions[0]["needs_triggers"] is True


# ---------------------------------------------------------------------------
# Tests: apply_upgrade
# ---------------------------------------------------------------------------


class TestApplyUpgrade:
    def test_adds_group_column(self, old_db):
        assert not _has_column(old_db, "_history_json_items", "group")
        apply_upgrade(old_db)
        assert _has_column(old_db, "_history_json_items", "group")

    def test_creates_groups_table(self, old_db):
        """The shared _history_json table should be created."""
        apply_upgrade(old_db)
        exists = old_db.execute(
            "select count(*) from sqlite_master "
            "where type = 'table' and name = '_history_json'"
        ).fetchone()[0]
        assert exists == 1

    def test_existing_rows_have_null_group(self, old_db):
        """Pre-existing audit rows should have group = NULL."""
        apply_upgrade(old_db)
        rows = old_db.execute(
            "select [group] from [_history_json_items]"
        ).fetchall()
        assert all(r[0] is None for r in rows)

    def test_preserves_existing_history(self, old_db):
        """Upgrade should not lose any existing audit data."""
        before = old_db.execute(
            "select id, timestamp, operation, pk_id, updated_values "
            "from [_history_json_items] order by id"
        ).fetchall()
        apply_upgrade(old_db)
        after = old_db.execute(
            "select id, timestamp, operation, pk_id, updated_values "
            "from [_history_json_items] order by id"
        ).fetchall()
        assert before == after

    def test_triggers_recreated(self, old_db):
        """After upgrade, triggers should include the [group] subquery."""
        apply_upgrade(old_db)
        assert not _trigger_needs_upgrade(old_db, "_history_json_items")

    def test_new_inserts_work_after_upgrade(self, old_db):
        """INSERTs after upgrade should succeed and have group = NULL."""
        apply_upgrade(old_db)
        old_db.execute(
            "insert into items (id, name, price, quantity) "
            "values (2, 'Gadget', 5.99, 50)"
        )
        row = old_db.execute(
            "select [group] from [_history_json_items] order by id desc limit 1"
        ).fetchone()
        assert row[0] is None

    def test_new_updates_work_after_upgrade(self, old_db):
        apply_upgrade(old_db)
        old_db.execute("update items set name = 'SuperWidget' where id = 1")
        row = old_db.execute(
            "select operation, [group] from [_history_json_items] "
            "order by id desc limit 1"
        ).fetchone()
        assert row[0] == "update"
        assert row[1] is None

    def test_new_deletes_work_after_upgrade(self, old_db):
        apply_upgrade(old_db)
        old_db.execute("delete from items where id = 1")
        row = old_db.execute(
            "select operation, [group] from [_history_json_items] "
            "order by id desc limit 1"
        ).fetchone()
        assert row[0] == "delete"
        assert row[1] is None

    def test_change_group_works_after_upgrade(self, old_db):
        """After upgrade, change_group() should correctly tag new entries."""
        apply_upgrade(old_db)
        with change_group(old_db, note="post-upgrade batch") as gid:
            old_db.execute(
                "insert into items (id, name, price, quantity) "
                "values (3, 'Thingamajig', 1.99, 300)"
            )
            old_db.execute("update items set price = 15.99 where id = 1")
        rows = old_db.execute(
            "select [group] from [_history_json_items] "
            "where [group] is not null"
        ).fetchall()
        assert len(rows) == 2
        assert all(r[0] == gid for r in rows)

    def test_get_history_works_after_upgrade(self, old_db):
        """get_history() should work correctly on upgraded databases."""
        apply_upgrade(old_db)
        entries = get_history(old_db, "items")
        assert len(entries) == 2
        assert entries[0]["group"] is None
        assert entries[0]["group_note"] is None

    def test_get_row_history_works_after_upgrade(self, old_db):
        apply_upgrade(old_db)
        entries = get_row_history(old_db, "items", {"id": 1})
        assert len(entries) == 2
        for e in entries:
            assert "group" in e
            assert "group_note" in e

    def test_upgrades_multiple_tables(self, old_db_multi):
        actions = apply_upgrade(old_db_multi)
        assert len(actions) == 2
        for table in ("_history_json_items", "_history_json_orders"):
            assert _has_column(old_db_multi, table, "group")

    def test_upgrades_compound_pk(self, old_db_compound_pk):
        apply_upgrade(old_db_compound_pk)
        assert _has_column(
            old_db_compound_pk, "_history_json_user_roles", "group"
        )
        # Verify triggers work
        old_db_compound_pk.execute(
            "insert into user_roles (user_id, role_id, granted_by) "
            "values (2, 20, 'admin')"
        )
        row = old_db_compound_pk.execute(
            "select [group] from [_history_json_user_roles] "
            "order by id desc limit 1"
        ).fetchone()
        assert row[0] is None

    def test_idempotent(self, old_db):
        """Running upgrade twice should be safe."""
        actions1 = apply_upgrade(old_db)
        assert len(actions1) == 1
        actions2 = apply_upgrade(old_db)
        assert actions2 == []

    def test_returns_empty_for_current_schema(self, conn):
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        enable_tracking(conn, "items")
        actions = apply_upgrade(conn)
        assert actions == []

    def test_mixed_old_and_new_tables(self, conn):
        """Database with one old and one new tracked table."""
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        conn.execute(
            "create table orders (id integer primary key, item_id integer)"
        )
        # items: old-style
        _create_old_style_tracking(conn, "items")
        # orders: current-style
        enable_tracking(conn, "orders")

        actions = apply_upgrade(conn)
        assert len(actions) == 1
        assert actions[0]["audit_table"] == "_history_json_items"

    def test_upgrades_v1_triggers_to_versioned(self, v1_db):
        """v1-style triggers should be replaced with versioned triggers."""
        apply_upgrade(v1_db)
        triggers = v1_db.execute(
            "select name from sqlite_master where type='trigger' and tbl_name='items'"
        ).fetchall()
        trigger_names = sorted(r[0] for r in triggers)
        expected = sorted([
            f"history_json_v{_TRIGGER_VERSION}_insert_items",
            f"history_json_v{_TRIGGER_VERSION}_update_items",
            f"history_json_v{_TRIGGER_VERSION}_delete_items",
        ])
        assert trigger_names == expected

    def test_v1_upgrade_preserves_history(self, v1_db):
        """Upgrading from v1 should preserve existing audit data."""
        before = v1_db.execute(
            "select id, timestamp, operation, pk_id, updated_values "
            "from [_history_json_items] order by id"
        ).fetchall()
        apply_upgrade(v1_db)
        after = v1_db.execute(
            "select id, timestamp, operation, pk_id, updated_values "
            "from [_history_json_items] order by id"
        ).fetchall()
        assert before == after

    def test_v1_upgrade_old_triggers_removed(self, v1_db):
        """Old unversioned trigger names should be gone after upgrade."""
        apply_upgrade(v1_db)
        old_trigger = v1_db.execute(
            "select name from sqlite_master where type='trigger' "
            "and name = '_history_json_items_update'"
        ).fetchone()
        assert old_trigger is None

    def test_v1_upgrade_inserts_work(self, v1_db):
        """INSERTs should work after upgrading from v1."""
        apply_upgrade(v1_db)
        v1_db.execute(
            "insert into items (id, name, price, quantity) "
            "values (2, 'Gadget', 5.99, 50)"
        )
        row = v1_db.execute(
            "select [group] from [_history_json_items] order by id desc limit 1"
        ).fetchone()
        assert row[0] is None

    def test_v1_upgrade_idempotent(self, v1_db):
        """Upgrading v1 twice should be safe."""
        actions1 = apply_upgrade(v1_db)
        assert len(actions1) == 1
        actions2 = apply_upgrade(v1_db)
        assert actions2 == []

    def test_upgrade_with_existing_data_and_new_change_group(self, old_db):
        """Full round-trip: old data, upgrade, new grouped changes, query all."""
        apply_upgrade(old_db)

        # Add new grouped changes
        with change_group(old_db, note="batch") as gid:
            old_db.execute(
                "insert into items (id, name, price, quantity) "
                "values (2, 'Gadget', 5.99, 50)"
            )

        entries = get_history(old_db, "items")
        # Should have 3 entries: 2 old (null group) + 1 new (with group)
        assert len(entries) == 3
        grouped = [e for e in entries if e["group"] is not None]
        ungrouped = [e for e in entries if e["group"] is None]
        assert len(grouped) == 1
        assert grouped[0]["group"] == gid
        assert grouped[0]["group_note"] == "batch"
        assert len(ungrouped) == 2


# ---------------------------------------------------------------------------
# Tests: CLI (main function)
# ---------------------------------------------------------------------------


class TestCLI:
    def _make_old_db_file(self):
        """Create a temp file with an old-style tracked database."""
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        conn = sqlite3.connect(f.name)
        conn.execute(
            "create table items ("
            "id integer primary key, name text, price float)"
        )
        _create_old_style_tracking(conn, "items")
        conn.execute(
            "insert into items (id, name, price) values (1, 'Widget', 9.99)"
        )
        conn.commit()
        conn.close()
        return f.name

    def test_dry_run_reports_actions(self, capsys):
        db_path = self._make_old_db_file()
        main(["--dry-run", db_path])
        captured = capsys.readouterr()
        assert "Would upgrade _history_json_items" in captured.err
        assert "add [group] column" in captured.err
        assert "recreate triggers" in captured.err

        # Verify nothing was actually changed
        conn = sqlite3.connect(db_path)
        assert not _has_column(conn, "_history_json_items", "group")
        conn.close()

    def test_dry_run_nothing_to_do(self, capsys):
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        conn = sqlite3.connect(f.name)
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        enable_tracking(conn, "items")
        conn.commit()
        conn.close()

        main(["--dry-run", f.name])
        captured = capsys.readouterr()
        assert "Nothing to upgrade" in captured.err

    def test_actual_upgrade(self, capsys):
        db_path = self._make_old_db_file()
        main([db_path])
        captured = capsys.readouterr()
        assert "Upgraded _history_json_items" in captured.err
        assert "added [group] column" in captured.err
        assert "recreated triggers" in captured.err

        # Verify the upgrade was applied
        conn = sqlite3.connect(db_path)
        assert _has_column(conn, "_history_json_items", "group")
        conn.close()

    def test_nothing_to_upgrade(self, capsys):
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        conn = sqlite3.connect(f.name)
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        enable_tracking(conn, "items")
        conn.commit()
        conn.close()

        main([f.name])
        captured = capsys.readouterr()
        assert "Nothing to upgrade" in captured.err

    def test_runnable_as_module(self):
        """python -m sqlite_history_json.upgrade --help should work."""
        result = subprocess.run(
            [sys.executable, "-m", "sqlite_history_json.upgrade", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--dry-run" in result.stdout

    def test_dry_run_flag_before_database(self, capsys):
        """--dry-run can come before the database argument."""
        db_path = self._make_old_db_file()
        main(["--dry-run", db_path])
        captured = capsys.readouterr()
        assert "Would upgrade" in captured.err


# ---------------------------------------------------------------------------
# Tests: _find_audit_tables helper
# ---------------------------------------------------------------------------


class TestFindAuditTables:
    def test_finds_audit_tables(self, old_db_multi):
        tables = _find_audit_tables(old_db_multi)
        assert set(tables) == {"_history_json_items", "_history_json_orders"}

    def test_does_not_include_groups_table(self, conn):
        """The _history_json groups table should not be returned."""
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        enable_tracking(conn, "items")
        tables = _find_audit_tables(conn)
        assert "_history_json" not in tables
        assert "_history_json_items" in tables

    def test_empty_database(self, conn):
        assert _find_audit_tables(conn) == []


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_audit_table_with_no_source_table(self, conn):
        """An orphaned audit table (source dropped) should still get the column."""
        conn.execute(
            "create table items (id integer primary key, name text)"
        )
        _create_old_style_tracking(conn, "items")
        conn.execute(
            "insert into items (id, name) values (1, 'Widget')"
        )
        # Drop source table (and its triggers)
        conn.execute("drop table items")

        actions = apply_upgrade(conn)
        assert len(actions) == 1
        assert actions[0]["needs_column"] is True
        # Triggers can't be recreated without source table
        assert actions[0]["needs_triggers"] is False
        # But column was added
        assert _has_column(conn, "_history_json_items", "group")

    def test_enable_tracking_on_upgraded_table(self, old_db):
        """After upgrade, calling enable_tracking again should be harmless."""
        apply_upgrade(old_db)
        # disable then re-enable
        from sqlite_history_json import disable_tracking

        disable_tracking(old_db, "items")
        enable_tracking(old_db, "items", populate_table=False)
        # Should still work
        old_db.execute(
            "insert into items (id, name, price, quantity) "
            "values (5, 'NewItem', 2.99, 10)"
        )
        entries = get_history(old_db, "items")
        assert entries[0]["operation"] == "insert"
        assert entries[0]["group"] is None
