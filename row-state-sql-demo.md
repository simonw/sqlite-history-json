# row-state-sql CLI Demo

*2026-02-08T05:40:10Z*

The `row-state-sql` command outputs a SQL query that can reconstruct a row's state at any point in its audit history, using a recursive CTE and `json_patch()`. Let's see it in action.

First, create a database with a simple table and enable tracking:

```bash
rm -f /tmp/demo_row_state.db
uv run python << 'PYEOF'
import sqlite3
conn = sqlite3.connect("/tmp/demo_row_state.db")
conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER)")
conn.commit()
conn.close()
print("Table created.")
PYEOF
uv run python -m sqlite_history_json /tmp/demo_row_state.db enable items

```

```output
Table created.
Tracking enabled for table 'items'.
```

Now insert a row, update it a couple of times, delete it, and re-insert to show the full lifecycle:

```bash
uv run python << 'PYEOF'
import sqlite3
conn = sqlite3.connect("/tmp/demo_row_state.db")
conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")
conn.execute("UPDATE items SET price = 12.99 WHERE id = 1")
conn.execute("UPDATE items SET name = 'Super Widget', quantity = 75 WHERE id = 1")
conn.execute("DELETE FROM items WHERE id = 1")
conn.execute("INSERT INTO items VALUES (1, 'Widget v2', 19.99, 50)")
conn.commit()
conn.close()
print("Done - 5 operations on row id=1")
PYEOF

```

```output
Done - 5 operations on row id=1
```

Let's check the audit log to see all the entries:

```bash
uv run python -m sqlite_history_json /tmp/demo_row_state.db history items
```

```output
[
  {
    "id": 5,
    "timestamp": "2026-02-08 05:41:27.139",
    "operation": "insert",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Widget v2",
      "price": 19.99,
      "quantity": 50
    }
  },
  {
    "id": 4,
    "timestamp": "2026-02-08 05:41:27.139",
    "operation": "delete",
    "pk": {
      "id": 1
    },
    "updated_values": null
  },
  {
    "id": 3,
    "timestamp": "2026-02-08 05:41:27.139",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Super Widget",
      "quantity": 75
    }
  },
  {
    "id": 2,
    "timestamp": "2026-02-08 05:41:27.139",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "price": 12.99
    }
  },
  {
    "id": 1,
    "timestamp": "2026-02-08 05:41:27.139",
    "operation": "insert",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Widget",
      "price": 9.99,
      "quantity": 100
    }
  }
]
```

Now use `row-state-sql` to get the reconstruction query for this table:

```bash
uv run python -m sqlite_history_json /tmp/demo_row_state.db row-state-sql items
```

```output
with entries as (
  select id, operation, updated_values,
         row_number() over (order by id) as rn
  from [_history_json_items]
  where [pk_id] = :pk
    and id <= :target_id
    and id >= (
      select max(id) from [_history_json_items]
      where [pk_id] = :pk
        and operation = 'insert' and id <= :target_id
    )
),
folded as (
  select rn, operation, updated_values as state
  from entries where rn = 1

  union all

  select e.rn, e.operation,
    case when e.operation = 'delete' then null
         else json_patch(f.state, e.updated_values)
    end
  from folded f
  join entries e on e.rn = f.rn + 1
)
select state from folded order by rn desc limit 1
```

This SQL query can be executed directly against the database with `:pk` and `:target_id` parameters. Let's use it to reconstruct the row at each audit version:

```bash
uv run python << 'PYEOF'
import sqlite3, json
from sqlite_history_json import row_state_sql

conn = sqlite3.connect("/tmp/demo_row_state.db")
sql = row_state_sql(conn, "items")

entries = conn.execute(
    "select id, operation from _history_json_items order by id"
).fetchall()

for entry_id, operation in entries:
    result = conn.execute(sql, {"pk": 1, "target_id": entry_id}).fetchone()
    if result is None:
        state_str = "(no history)"
    elif result[0] is None:
        state_str = "(deleted)"
    else:
        state_str = result[0]
    print(f"Version {entry_id} ({operation:>6s}): {state_str}")

conn.close()
PYEOF

```

```output
Version 1 (insert): {"name":"Widget","price":9.99,"quantity":100}
Version 2 (update): {"name":"Widget","price":12.99,"quantity":100}
Version 3 (update): {"name":"Super Widget","price":12.99,"quantity":75}
Version 4 (delete): (deleted)
Version 5 (insert): {"name":"Widget v2","price":19.99,"quantity":50}
```

The query correctly handles the full lifecycle: the initial insert, each update folding in only the changed values, the delete returning null, and the reinsert starting fresh.

If tracking is not enabled for a table, the command exits with an error:

```bash
uv run python << 'PYEOF'
import sqlite3
conn = sqlite3.connect("/tmp/demo_row_state.db")
conn.execute("CREATE TABLE IF NOT EXISTS untracked (id INTEGER PRIMARY KEY, val TEXT)")
conn.commit()
conn.close()
PYEOF
uv run python -m sqlite_history_json /tmp/demo_row_state.db row-state-sql untracked 2>&1; echo "Exit code: $?"

```

```output
Error: Tracking is not enabled for table 'untracked' (audit table '_history_json_untracked' does not exist).
Exit code: 1
```

Finally, let's demonstrate with a compound primary key table. The generated SQL uses numbered parameters `:pk_1`, `:pk_2`:

```bash
uv run python << 'PYEOF'
import sqlite3
conn = sqlite3.connect("/tmp/demo_row_state.db")
conn.execute("""
    CREATE TABLE user_roles (
        user_id INTEGER,
        role_id INTEGER,
        granted_by TEXT,
        active INTEGER,
        PRIMARY KEY (user_id, role_id)
    )
""")
conn.commit()
conn.close()
print("Compound PK table created.")
PYEOF
uv run python -m sqlite_history_json /tmp/demo_row_state.db enable user_roles
uv run python -m sqlite_history_json /tmp/demo_row_state.db row-state-sql user_roles

```

```output
Compound PK table created.
Tracking enabled for table 'user_roles'.
with entries as (
  select id, operation, updated_values,
         row_number() over (order by id) as rn
  from [_history_json_user_roles]
  where [pk_user_id] = :pk_1 and [pk_role_id] = :pk_2
    and id <= :target_id
    and id >= (
      select max(id) from [_history_json_user_roles]
      where [pk_user_id] = :pk_1 and [pk_role_id] = :pk_2
        and operation = 'insert' and id <= :target_id
    )
),
folded as (
  select rn, operation, updated_values as state
  from entries where rn = 1

  union all

  select e.rn, e.operation,
    case when e.operation = 'delete' then null
         else json_patch(f.state, e.updated_values)
    end
  from folded f
  join entries e on e.rn = f.rn + 1
)
select state from folded order by rn desc limit 1
```
