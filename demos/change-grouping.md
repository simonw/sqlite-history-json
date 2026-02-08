# Change Grouping with Notes

*2026-02-08T13:41:35Z*

sqlite-history-json now supports grouping related changes together and attaching notes to those groups. This is useful for tagging batch operations like imports, migrations, or multi-step updates so you can later understand *why* a set of changes were made, not just *what* changed.

The mechanism uses a shared `_history_json` table with a `current` sentinel row. During a `change_group()` context, triggers automatically pick up the active group id via a subquery. Outside the context, audit rows have `group = NULL`.

## Setup

Create a database, a table, and enable tracking.

```python

import sqlite3
from sqlite_history_json import enable_tracking, get_history, change_group

conn = sqlite3.connect(':memory:')
conn.execute('''CREATE TABLE items (
    id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER
)''')
enable_tracking(conn, 'items')

print('Tables created:')
for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(f'  {row[0]}')

```

```output
Tables created:
  _history_json
  _history_json_items
  items
```

Both the audit table `_history_json_items` and the shared groups table `_history_json` are created.

## Changes without a group

Regular changes have `group = NULL` in the audit log.

```python

import sqlite3
from sqlite_history_json import enable_tracking, get_history, change_group

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER)')
enable_tracking(conn, 'items')

conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")

for e in get_history(conn, 'items'):
    print(f'op={e["operation"]:6s}  group={e["group"]}  note={e["group_note"]}  vals={e["updated_values"]}')

```

```output
op=insert  group=None  note=None  vals={'name': 'Widget', 'price': 9.99, 'quantity': 100}
```

## Grouping changes with a note

`change_group()` is a context manager. Everything inside the block shares the same group id.

```python

import sqlite3
from sqlite_history_json import enable_tracking, get_history, change_group

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER)')
enable_tracking(conn, 'items')

with change_group(conn, note='bulk import from warehouse CSV') as group_id:
    conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")
    conn.execute("INSERT INTO items VALUES (2, 'Gadget', 24.99, 50)")
    conn.execute("UPDATE items SET price = 12.99 WHERE id = 1")

print(f'Group ID: {group_id}')
print()
for e in get_history(conn, 'items'):
    print(f'id={e["id"]}  op={e["operation"]:6s}  group={e["group"]}  note={e["group_note"]!r}')

```

```output
Group ID: 1

id=3  op=update  group=1  note='bulk import from warehouse CSV'
id=2  op=insert  group=1  note='bulk import from warehouse CSV'
id=1  op=insert  group=1  note='bulk import from warehouse CSV'
```

All three audit entries share group 1 and the note.

## Multiple groups stay distinct

Each `change_group()` block gets its own id. Ungrouped changes in between have `group=None`.

```python

import sqlite3
from sqlite_history_json import enable_tracking, get_history, change_group

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, quantity INTEGER)')
enable_tracking(conn, 'items')

with change_group(conn, note='initial stock'):
    conn.execute("INSERT INTO items VALUES (1, 'Widget', 9.99, 100)")

conn.execute("UPDATE items SET price = 7.99 WHERE id = 1")  # ungrouped

with change_group(conn, note='holiday sale pricing'):
    conn.execute("UPDATE items SET price = 4.99 WHERE id = 1")
    conn.execute("UPDATE items SET quantity = 200 WHERE id = 1")

for e in get_history(conn, 'items'):
    print(f'id={e["id"]}  op={e["operation"]:6s}  group={str(e["group"]):>4s}  note={e["group_note"]}')

```

```output
id=4  op=update  group=   2  note=holiday sale pricing
id=3  op=update  group=   2  note=holiday sale pricing
id=2  op=update  group=None  note=None
id=1  op=insert  group=   1  note=initial stock
```

## Cross-table grouping

A single `change_group()` block groups changes across all tracked tables.

```python

import sqlite3
from sqlite_history_json import enable_tracking, get_history, change_group

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
conn.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)')
enable_tracking(conn, 'items')
enable_tracking(conn, 'orders')

with change_group(conn, note='new product launch') as group_id:
    conn.execute("INSERT INTO items VALUES (1, 'SuperWidget')")
    conn.execute("INSERT INTO orders VALUES (1, 1)")

for table in ['items', 'orders']:
    entries = get_history(conn, table)
    for e in entries:
        print(f'{table:6s}  op={e["operation"]:6s}  group={e["group"]}  note={e["group_note"]!r}')

```

```output
items   op=insert  group=1  note='new product launch'
orders  op=insert  group=1  note='new product launch'
```

## The trigger SQL

Here is the actual INSERT trigger SQL generated for the `items` table. The key addition is the subquery at the end that looks up the active group:

```python

import sqlite3
from sqlite_history_json import enable_tracking

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)')
enable_tracking(conn, 'items')

row = conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='_history_json_items_insert'"
).fetchone()
print(row[0])

```

```output
CREATE TRIGGER [_history_json_items_insert]
after insert on [items]
begin
    insert into [_history_json_items] (timestamp, operation, [pk_id], updated_values, [group])
    values (
        strftime('%Y-%m-%d %H:%M:%f', 'now'),
        'insert',
        NEW.[id],
        json_object('name', case when NEW.[name] is null then json_object('null', 1) else NEW.[name] end, 'price', case when NEW.[price] is null then json_object('null', 1) else NEW.[price] end),
        (select id from [_history_json] where current = 1)
    );
end
```

The groups table and audit table schemas:

```python

import sqlite3
from sqlite_history_json import enable_tracking

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
enable_tracking(conn, 'items')

for name in ['_history_json', '_history_json_items']:
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", [name]).fetchone()
    print(row[0])
    print()

```

```output
CREATE TABLE [_history_json] (
    id integer primary key,
    note text,
    current integer
)

CREATE TABLE [_history_json_items] (
    id integer primary key,
    timestamp text,
    operation text,
    [pk_id] INTEGER,
    updated_values text,
    [group] integer references [_history_json](id)
)

```

The `[group]` column on the audit table references `_history_json(id)`. The `current` column on the groups table is indexed and acts as the sentinel: set to 1 during an active `change_group()` block, NULL otherwise.
