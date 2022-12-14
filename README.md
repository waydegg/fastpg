# fastpg

[![PyPI version](https://badge.fury.io/py/fastpg.svg)](https://pypi.org/project/fastpg/)

A fast Postgres client library for Python.

The only dependency for **fastpg** is [asyncpg](https://github.com/MagicStack/asyncpg),
and the api wrapper is very similar to the api provided by the
[databases](https://github.com/encode/databases) package.

## Installation

```bash
$ pip install fastpg
```

## Quickstart

```python
from fastpg import Database

database = Database("postgresql://username:password@localhost:5432/postgres")
await database.connect()

# Execute a query
query = """
  create table high_scores (
    id uuid primary key default gen_random_uuid(),
    name varchar(100) not null,
    score integer not null
  )
"""
await database.execute(query)

# Execute many queries
query = "insert into high_scores (name, score) values (:name, :score)"
values = [
  {"name": "George", "score": 43},
  {"name": "Jess", "score": 67},
  {"name": "Kevin", "score": 30}
]
await database.execute_many(query, values)

# Fetch many rows
query = "select * from high_scores"
records = await database.fetch_many(query)
assert(len(records) == 3)

# Fetch a row
query = "select * from high_scores where name = :name"
values = {"name": "George"}
record = await database.fetch_one(query, values)
assert(record.score == 43)

# Fetch a value
query = "select name from high_scores where score = :score"
values = {"score": 30}
value = await database.fetch_val(query, values)
assert(value == "Kevin")

# Copy records to a table (via Postgres COPY)
await database.copy_records_to_table(
  "high_scores",
  records=[("Mav", 200), ("Connor", 134)],
  columns=["name", "score"]
)

# Work with individual connections
async with database.connection() as connection:
  ...

  # Create a transaction
  async with connection.transaction():
    ...

await database.disconnect()
```
