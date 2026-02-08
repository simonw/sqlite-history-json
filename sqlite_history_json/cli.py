"""Command-line interface for sqlite-history-json."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .core import (
    _audit_table_name,
    _get_pk_columns,
    _get_table_info,
    disable_tracking,
    enable_tracking,
    get_history,
    get_row_history,
    restore,
    row_state_sql,
)


def _coerce_value(s: str):
    """Try to coerce a string to int, then float, otherwise keep as str."""
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def cmd_enable(args):
    conn = sqlite3.connect(args.database)
    try:
        enable_tracking(conn, args.table, populate_table=not args.no_populate)
        conn.commit()
        print(f"Tracking enabled for table '{args.table}'.", file=sys.stderr)
    finally:
        conn.close()


def cmd_disable(args):
    conn = sqlite3.connect(args.database)
    try:
        disable_tracking(conn, args.table)
        conn.commit()
        print(f"Tracking disabled for table '{args.table}'.", file=sys.stderr)
    finally:
        conn.close()


def cmd_history(args):
    conn = sqlite3.connect(args.database)
    try:
        entries = get_history(conn, args.table, limit=args.n)
        json.dump(entries, sys.stdout, indent=2)
        sys.stdout.write("\n")
    finally:
        conn.close()


def cmd_row_history(args):
    conn = sqlite3.connect(args.database)
    try:
        columns = _get_table_info(conn, args.table)
        pk_cols = _get_pk_columns(columns)
        if len(args.pk_values) != len(pk_cols):
            pk_names = [c["name"] for c in pk_cols]
            print(
                f"Error: table '{args.table}' has {len(pk_cols)} primary key "
                f"column(s) ({', '.join(pk_names)}), but {len(args.pk_values)} "
                f"value(s) provided.",
                file=sys.stderr,
            )
            sys.exit(1)

        pk_values = {}
        for col, val_str in zip(pk_cols, args.pk_values):
            pk_values[col["name"]] = _coerce_value(val_str)

        entries = get_row_history(conn, args.table, pk_values, limit=args.n)
        json.dump(entries, sys.stdout, indent=2)
        sys.stdout.write("\n")
    finally:
        conn.close()


def cmd_restore(args):
    conn = sqlite3.connect(args.database)
    try:
        restore_kwargs: dict = {}
        if args.timestamp is not None:
            restore_kwargs["timestamp"] = args.timestamp
        if args.id is not None:
            restore_kwargs["up_to_id"] = args.id

        if args.replace_table:
            restore_kwargs["swap"] = True
        elif args.new_table:
            restore_kwargs["new_table_name"] = args.new_table

        if args.output_db:
            # Restore to a temp table, then copy to the output database
            temp_name = f"_cli_restore_tmp_{args.table}"
            restore_kwargs["new_table_name"] = temp_name
            restore_kwargs.pop("swap", None)

            restored = restore(conn, args.table, **restore_kwargs)

            # Get the original CREATE TABLE SQL and adapt it
            create_sql = conn.execute(
                "select sql from sqlite_master where type='table' and name=?",
                (args.table,),
            ).fetchone()[0]

            conn.execute(
                "attach database ? as output_db", (args.output_db,)
            )
            try:
                # Create the table in the output database
                # Replace the table name in the CREATE statement
                output_table = args.table
                target_create = create_sql.replace(
                    f"CREATE TABLE {args.table}",
                    f"CREATE TABLE [output_db].[{output_table}]",
                    1,
                )
                if target_create == create_sql:
                    target_create = create_sql.replace(
                        f'CREATE TABLE "{args.table}"',
                        f"CREATE TABLE [output_db].[{output_table}]",
                        1,
                    )
                if target_create == create_sql:
                    target_create = create_sql.replace(
                        f"CREATE TABLE [{args.table}]",
                        f"CREATE TABLE [output_db].[{output_table}]",
                        1,
                    )
                conn.execute(target_create)

                # Copy data
                columns = _get_table_info(conn, args.table)
                col_names = ", ".join(f"[{c['name']}]" for c in columns)
                conn.execute(
                    f"insert into [output_db].[{output_table}] ({col_names}) "
                    f"select {col_names} from [{restored}]"
                )
                conn.commit()
                print(
                    f"Restored table '{args.table}' written to '{args.output_db}'.",
                    file=sys.stderr,
                )
            finally:
                conn.execute(f"drop table if exists [{restored}]")
                conn.execute("detach database output_db")
        else:
            restored = restore(conn, args.table, **restore_kwargs)
            conn.commit()
            if args.replace_table:
                print(
                    f"Table '{args.table}' replaced with restored data.",
                    file=sys.stderr,
                )
            else:
                print(
                    f"Restored table created as '{restored}'.",
                    file=sys.stderr,
                )
    finally:
        conn.close()


def cmd_row_state_sql(args):
    conn = sqlite3.connect(args.database)
    try:
        sql = row_state_sql(conn, args.table)
        sys.stdout.write(sql + "\n")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def cli(args=None):
    parser = argparse.ArgumentParser(
        prog="python -m sqlite_history_json",
        description="SQLite table history tracking using a JSON audit log.",
    )
    parser.add_argument("database", help="Path to the SQLite database file.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # enable
    p_enable = subparsers.add_parser("enable", help="Enable tracking for a table.")
    p_enable.add_argument("table", help="Table name to track.")
    p_enable.add_argument(
        "--no-populate",
        action="store_true",
        help="Skip populating the audit log with existing rows.",
    )
    p_enable.set_defaults(func=cmd_enable)

    # disable
    p_disable = subparsers.add_parser("disable", help="Disable tracking for a table.")
    p_disable.add_argument("table", help="Table name to stop tracking.")
    p_disable.set_defaults(func=cmd_disable)

    # history
    p_history = subparsers.add_parser(
        "history", help="Show audit log entries for a table."
    )
    p_history.add_argument("table", help="Table name.")
    p_history.add_argument(
        "-n", type=int, default=None, help="Maximum number of entries to show."
    )
    p_history.set_defaults(func=cmd_history)

    # row-history
    p_row_history = subparsers.add_parser(
        "row-history", help="Show audit log entries for a specific row."
    )
    p_row_history.add_argument("table", help="Table name.")
    p_row_history.add_argument(
        "pk_values",
        nargs="+",
        help="Primary key values in PK column order.",
    )
    p_row_history.add_argument(
        "-n", type=int, default=None, help="Maximum number of entries to show."
    )
    p_row_history.set_defaults(func=cmd_row_history)

    # restore
    p_restore = subparsers.add_parser(
        "restore", help="Restore a table from its audit log."
    )
    p_restore.add_argument("table", help="Table name to restore.")
    p_restore.add_argument(
        "--id", type=int, default=None, help="Restore up to this audit log entry ID."
    )
    p_restore.add_argument(
        "--timestamp", default=None, help="Restore up to this timestamp (inclusive)."
    )
    p_restore.add_argument(
        "--new-table", default=None, help="Name for the restored table."
    )
    restore_group = p_restore.add_mutually_exclusive_group()
    restore_group.add_argument(
        "--replace-table",
        action="store_true",
        help="Replace the original table with the restored version.",
    )
    restore_group.add_argument(
        "--output-db",
        default=None,
        help="Write the restored table to a different database file.",
    )
    p_restore.set_defaults(func=cmd_restore)

    # row-state-sql
    p_row_state_sql = subparsers.add_parser(
        "row-state-sql",
        help="Output the SQL query to reconstruct a row's state at a given version.",
    )
    p_row_state_sql.add_argument("table", help="Table name.")
    p_row_state_sql.set_defaults(func=cmd_row_state_sql)

    parsed = parser.parse_args(args)
    parsed.func(parsed)
