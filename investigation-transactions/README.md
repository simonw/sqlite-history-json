# Transaction behavior investigation: sqlite-history-json

## Question
What does this library do with respect to transactions?

## Findings

1. The library does **not** explicitly manage transactions.
   - No `conn.commit()` or `conn.rollback()` calls appear in `src/sqlite_history_json/core.py`.
   - It executes SQL statements using `conn.execute(...)` and relies on the caller's transaction context.

2. Change capture is implemented via `AFTER INSERT/UPDATE/DELETE` triggers.
   - Trigger SQL is generated in:
     - `_build_insert_trigger_sql()` (`src/sqlite_history_json/core.py:48`)
     - `_build_update_trigger_sql()` (`src/sqlite_history_json/core.py:93`)
     - `_build_delete_trigger_sql()` (`src/sqlite_history_json/core.py:153`)
   - These triggers insert audit rows into `_history_json_<table>` in the same statement context.

3. `enable_tracking()` setup operations are also uncommitted until caller commit.
   - It runs `CREATE TABLE`, `CREATE TRIGGER`, and `CREATE INDEX` via `conn.execute(...)` (`src/sqlite_history_json/core.py:216-250`).
   - No explicit commit is performed afterward.

4. Runtime probe confirms transaction coupling.
   - Insert into tracked table within `BEGIN ... ROLLBACK` produced no audit row.
   - Insert within `BEGIN ... COMMIT` produced audit row.
   - Update+delete within `BEGIN ... ROLLBACK` left audit table unchanged.
   - Calling `enable_tracking()` inside `BEGIN ... ROLLBACK` left no audit table/triggers.

## Practical interpretation
The audit log is transactionally consistent with the source table:
- if a transaction rolls back, tracked-row changes and corresponding audit rows roll back together;
- if a transaction commits, both commit together.

So this library does transaction-safe auditing by delegating transaction boundaries to SQLite + the caller, rather than introducing its own transaction management layer.
