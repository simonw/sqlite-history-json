# sqlite-history-json CLI demo

*2026-02-08T05:07:38Z*

sqlite-history-json includes a CLI that can be run via `python -m sqlite_history_json`. This demo walks through its main commands.

Start by viewing the top-level help:

```bash
python -m sqlite_history_json --help
```

```output
usage: python -m sqlite_history_json [-h]
                                     database
                                     {enable,disable,history,row-history,restore} ...

SQLite table history tracking using a JSON audit log.

positional arguments:
  database              Path to the SQLite database file.
  {enable,disable,history,row-history,restore}
    enable              Enable tracking for a table.
    disable             Disable tracking for a table.
    history             Show audit log entries for a table.
    row-history         Show audit log entries for a specific row.
    restore             Restore a table from its audit log.

options:
  -h, --help            show this help message and exit
```

Create a test database with an `items` table:

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/tmp/cli-demo.db')
conn.execute('''CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    name TEXT,
    price FLOAT,
    quantity INTEGER
)''')
conn.executemany(
    'INSERT INTO items VALUES (?, ?, ?, ?)',
    [(1, 'Widget', 9.99, 100), (2, 'Gadget', 24.99, 50), (3, 'Doohickey', 4.99, 200)]
)
conn.commit()
print('Created items table with 3 rows')
"
```

```output
Created items table with 3 rows
```

## Enable tracking

Enable history tracking on the items table. This creates the audit table and populates it with a snapshot of existing rows:

```bash
python -m sqlite_history_json /tmp/cli-demo.db enable items 2>&1
```

```output
Tracking enabled for table 'items'.
```

Now make some changes to the table — update a row and delete another:

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/tmp/cli-demo.db')
conn.execute(\"UPDATE items SET price = 12.99, name = 'Super Widget' WHERE id = 1\")
conn.execute(\"DELETE FROM items WHERE id = 3\")
conn.commit()
print('Updated item 1 and deleted item 3')
"
```

```output
Updated item 1 and deleted item 3
```

## View history

The `history` command shows all audit log entries for the table as JSON, newest first:

```bash
python -m sqlite_history_json /tmp/cli-demo.db history items
```

```output
[
  {
    "id": 5,
    "timestamp": "2026-02-08 05:09:43.201",
    "operation": "delete",
    "pk": {
      "id": 3
    },
    "updated_values": null
  },
  {
    "id": 4,
    "timestamp": "2026-02-08 05:09:43.201",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Super Widget",
      "price": 12.99
    }
  },
  {
    "id": 3,
    "timestamp": "2026-02-08 05:09:43.175",
    "operation": "insert",
    "pk": {
      "id": 3
    },
    "updated_values": {
      "name": "Doohickey",
      "price": 4.99,
      "quantity": 200
    }
  },
  {
    "id": 2,
    "timestamp": "2026-02-08 05:09:43.175",
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
    "timestamp": "2026-02-08 05:09:43.175",
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

Use `-n` to limit the number of entries:

```bash
python -m sqlite_history_json /tmp/cli-demo.db history items -n 2
```

```output
[
  {
    "id": 5,
    "timestamp": "2026-02-08 05:09:43.201",
    "operation": "delete",
    "pk": {
      "id": 3
    },
    "updated_values": null
  },
  {
    "id": 4,
    "timestamp": "2026-02-08 05:09:43.201",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Super Widget",
      "price": 12.99
    }
  }
]
```

## Row history

The `row-history` command filters to a specific row by its primary key value:

```bash
python -m sqlite_history_json /tmp/cli-demo.db row-history items 1
```

```output
[
  {
    "id": 4,
    "timestamp": "2026-02-08 05:09:43.201",
    "operation": "update",
    "pk": {
      "id": 1
    },
    "updated_values": {
      "name": "Super Widget",
      "price": 12.99
    }
  },
  {
    "id": 1,
    "timestamp": "2026-02-08 05:09:43.175",
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

## Restore

Restore the table to an earlier state. By default this creates a new table called `items_restored`:

```bash
python -m sqlite_history_json /tmp/cli-demo.db restore items --id 3 2>&1
```

```output
Restored table created as 'items_restored'.
```

The restored table has the data as it existed after the initial population (audit entries 1-3), before any updates or deletes:

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('/tmp/cli-demo.db')
rows = conn.execute('SELECT * FROM items_restored ORDER BY id').fetchall()
for r in rows:
    print(dict(zip(['id', 'name', 'price', 'quantity'], r)))
"
```

```output
{'id': 1, 'name': 'Widget', 'price': 9.99, 'quantity': 100}
{'id': 2, 'name': 'Gadget', 'price': 24.99, 'quantity': 50}
{'id': 3, 'name': 'Doohickey', 'price': 4.99, 'quantity': 200}
```

## Restore to a different database

Use `--output-db` to write the restored table to a separate database file:

```bash
python -m sqlite_history_json /tmp/cli-demo.db restore items --id 3 --output-db /tmp/cli-demo-backup.db 2>&1
```

```output
Restored table 'items' written to '/tmp/cli-demo-backup.db'.
```

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/tmp/cli-demo-backup.db')
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print('Tables in backup.db:', tables)
rows = conn.execute('SELECT * FROM items ORDER BY id').fetchall()
for r in rows:
    print(dict(zip(['id', 'name', 'price', 'quantity'], r)))
"
```

```output
Tables in backup.db: ['items']
{'id': 1, 'name': 'Widget', 'price': 9.99, 'quantity': 100}
{'id': 2, 'name': 'Gadget', 'price': 24.99, 'quantity': 50}
{'id': 3, 'name': 'Doohickey', 'price': 4.99, 'quantity': 200}
```

## Disable tracking

Disable tracking drops the triggers but preserves the audit table:

```bash
python -m sqlite_history_json /tmp/cli-demo.db disable items 2>&1
```

```output
Tracking disabled for table 'items'.
```
