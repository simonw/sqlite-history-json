# Upgrading older sqlite-history-json databases

*2026-02-09T00:41:08Z*

Databases created with sqlite-history-json 0.3a0 or earlier use an older audit table schema that doesn't include the `[group]` column for change grouping. This demo shows how to upgrade them.

First, let's create a database and enable tracking using the published 0.3a0 release (which predates change-grouping support).

```bash
uv run --no-project --with "sqlite-history-json==0.3a0" python -c "
import sqlite3
from sqlite_history_json import enable_tracking
conn = sqlite3.connect(\"demo.db\")
conn.execute(\"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER)\")
enable_tracking(conn, \"items\")
conn.commit()
conn.close()
print(\"Database created with tracking enabled.\")
"
```

```output
Database created with tracking enabled.
```

Now let's make some changes to generate audit history.

```bash
uv run --no-project --with "sqlite-history-json==0.3a0" python -c "
import sqlite3
conn = sqlite3.connect(\"demo.db\")
conn.execute(\"INSERT INTO items VALUES (1, 'Widget', 9.99, 100)\")
conn.execute(\"INSERT INTO items VALUES (2, 'Gadget', 24.99, 50)\")
conn.execute(\"UPDATE items SET price = 12.99 WHERE id = 1\")
conn.execute(\"DELETE FROM items WHERE id = 2\")
conn.commit()
conn.close()
print(\"Changes applied.\")
"
```

```output
Changes applied.
```

Let's look at the audit history. Note there are no `group` or `group_note` fields.

```bash
uv run --no-project --with "sqlite-history-json==0.3a0" python -c "
import sqlite3, json
from sqlite_history_json import get_history
conn = sqlite3.connect(\"demo.db\")
conn.row_factory = sqlite3.Row
entries = get_history(conn, \"items\")
print(json.dumps(entries, indent=2))
conn.close()
"
```

```output
[
  {
    "id": 4,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "delete",
    "pk": {
      "id": 2
    },
    "updated_values": null
  },
  {
    "id": 3,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "price": 12.99
    }
  },
  {
    "id": 2,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "insert",
    "pk": {
      "id": 2
    },
    "updated_values": {
      "name": "Gadget",
      "price": 24.99,
      "quantity": 50
    }
  },
  {
    "id": 1,
    "timestamp": "2026-02-09 00:41:49.233",
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

And here's the audit table schema — no `[group]` column, and no `_history_json` groups table.

```bash
uv run --no-project --with "sqlite-history-json==0.3a0" python -c "
import sqlite3
conn = sqlite3.connect(\"demo.db\")
cols = [r[1] for r in conn.execute(\"PRAGMA table_info(_history_json_items)\").fetchall()]
print(\"Audit table columns:\", cols)
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()]
print(\"All tables:\", tables)
conn.close()
"
```

```output
Audit table columns: ['id', 'timestamp', 'operation', 'pk_id', 'updated_values']
All tables: ['_history_json_items', 'items']
```

Now let's run the upgrade script. First with `--dry-run` to see what it would do.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -m sqlite_history_json.upgrade demo.db --dry-run
```

```output
Would upgrade _history_json_items: add [group] column, recreate triggers
```

Looks good. Let's apply the upgrade for real.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -m sqlite_history_json.upgrade demo.db
```

```output
Upgraded _history_json_items: added [group] column, recreated triggers
```

Let's verify the schema has been updated. The audit table now has a `[group]` column, and the `_history_json` groups table exists.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -c "
import sqlite3
conn = sqlite3.connect(\"demo.db\")
cols = [r[1] for r in conn.execute(\"PRAGMA table_info(_history_json_items)\").fetchall()]
print(\"Audit table columns:\", cols)
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()]
print(\"All tables:\", tables)
conn.close()
"
```

```output
Audit table columns: ['id', 'timestamp', 'operation', 'pk_id', 'updated_values', 'group']
All tables: ['_history_json', '_history_json_items', 'items']
```

Existing audit data is preserved — the old rows now have `group = NULL`.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -c "
import sqlite3, json
from sqlite_history_json import get_history
conn = sqlite3.connect(\"demo.db\")
conn.row_factory = sqlite3.Row
entries = get_history(conn, \"items\")
print(json.dumps(entries, indent=2))
conn.close()
"
```

```output
[
  {
    "id": 4,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "delete",
    "pk": {
      "id": 2
    },
    "updated_values": null,
    "group": null,
    "group_note": null
  },
  {
    "id": 3,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "price": 12.99
    },
    "group": null,
    "group_note": null
  },
  {
    "id": 2,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "insert",
    "pk": {
      "id": 2
    },
    "updated_values": {
      "name": "Gadget",
      "price": 24.99,
      "quantity": 50
    },
    "group": null,
    "group_note": null
  },
  {
    "id": 1,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "insert",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Widget",
      "price": 9.99,
      "quantity": 100
    },
    "group": null,
    "group_note": null
  }
]
```

Now we can use `change_group()` to group new changes together with a note.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -c "
import sqlite3, json
from sqlite_history_json import change_group, get_history
conn = sqlite3.connect(\"demo.db\")
conn.row_factory = sqlite3.Row
with change_group(conn, note=\"price adjustment\"):
    conn.execute(\"UPDATE items SET price = 14.99 WHERE id = 1\")
    conn.execute(\"INSERT INTO items VALUES (3, 'Doohickey', 4.99, 200)\")
conn.commit()
entries = get_history(conn, \"items\")
print(json.dumps(entries, indent=2))
conn.close()
"
```

```output
[
  {
    "id": 6,
    "timestamp": "2026-02-09 00:43:47.193",
    "operation": "insert",
    "pk": {
      "id": 3
    },
    "updated_values": {
      "name": "Doohickey",
      "price": 4.99,
      "quantity": 200
    },
    "group": 1,
    "group_note": "price adjustment"
  },
  {
    "id": 5,
    "timestamp": "2026-02-09 00:43:47.193",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "price": 14.99
    },
    "group": 1,
    "group_note": "price adjustment"
  },
  {
    "id": 4,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "delete",
    "pk": {
      "id": 2
    },
    "updated_values": null,
    "group": null,
    "group_note": null
  },
  {
    "id": 3,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "price": 12.99
    },
    "group": null,
    "group_note": null
  },
  {
    "id": 2,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "insert",
    "pk": {
      "id": 2
    },
    "updated_values": {
      "name": "Gadget",
      "price": 24.99,
      "quantity": 50
    },
    "group": null,
    "group_note": null
  },
  {
    "id": 1,
    "timestamp": "2026-02-09 00:41:49.233",
    "operation": "insert",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Widget",
      "price": 9.99,
      "quantity": 100
    },
    "group": null,
    "group_note": null
  }
]
```

The two newest entries share the same `group` ID and `group_note`, while the older pre-upgrade entries remain with `group: null`. The upgrade is complete — old history is preserved and new grouping features work.

Running the upgrade again is safe — it detects nothing needs to be done.

```bash
PYTHONPATH=/home/user/sqlite-history-json python -m sqlite_history_json.upgrade demo.db
```

```output
Nothing to upgrade.
```
