# FlowDB

> **A high-performance database engine built in Rust with its own query language — FlowQL.**

[![Version](https://img.shields.io/badge/version-0.2.0-blue)](https://github.com/Ajaikumar0712/Flow_DB)
[![License](https://img.shields.io/badge/license-BUSL--1.1-orange)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey)](https://github.com/Ajaikumar0712/Flow_DB/releases)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org)

FlowDB is a self-contained, production-ready database engine that stores data in-memory with WAL-backed durability, communicates over TCP using JSON, and is queried with **FlowQL** — a purpose-built, SQL-inspired query language.

**Made by Ajaikumar · [ForNexus](https://fornexus.tech)**

---

## At a Glance

| Feature | Description |
|---|---|
| **FlowQL** | Custom query language — clean, readable, not SQL |
| **In-memory + WAL** | Fast reads/writes with Write-Ahead Log durability |
| **Joins** | INNER, LEFT, RIGHT joins across tables in one query |
| **Aggregation** | GROUP BY with COUNT, SUM, AVG, MIN, MAX and HAVING |
| **Fuzzy Search** | Trigram-based text similarity search (`~`) |
| **Vector Search** | Cosine similarity search with HNSW index (`SIMILAR`) |
| **Streaming** | Real-time push subscriptions with `WATCH` |
| **Transactions** | `BEGIN` / `COMMIT` / `ROLLBACK` with MVCC isolation |
| **Security** | Users, SHA-256 passwords, roles, per-table permissions |
| **Clustering** | Peer heartbeats, Raft consensus, FNV-1a shard routing |
| **REST API** | Auto-generated HTTP endpoint per table (`API GENERATE`) |
| **Windows Service** | Runs as a background service that starts with Windows |
| **MSI Installer** | One-click install on Windows |

---

## Quick Start

### 1 · Install

Download **`flowdb-0.2.0-x86_64.msi`** and double-click to install.

This places `flowdb-server.exe` and `flowdb-repl.exe` in:
```
C:\Program Files\FlowDB\bin\
```
That folder is automatically added to your system `PATH`.

### 2 · Start the Server

Open **any terminal** and run:

```powershell
flowdb-server
```

You should see:
```
[INFO] FlowDB 0.2.0 listening on 127.0.0.1:7878
```

### 3 · Connect and Query

Open a **second terminal** and run:

```powershell
flowdb-repl
```

At the `flowql>` prompt, try:

```sql
MAKE TABLE users (id int PRIMARY KEY, name text, age int)

PUT users { id: 1, name: "Alice", age: 30 }
PUT users { id: 2, name: "Bob",   age: 25 }

GET users
GET users WHERE age >= 28 ORDER BY name ASC
```

That's it — you're running FlowDB.

---

## Table of Contents

1. [Installation](#installation)
   - [MSI Installer (Windows)](#option-a--msi-installer-windows)
   - [Build from Source](#option-b--build-from-source)
2. [Running the Server](#running-the-server)
3. [Windows Service](#windows-service)
4. [Connecting with the REPL](#connecting-with-the-repl)
5. [FlowQL Language Reference](#flowql-language-reference)
   - [Data Types](#data-types)
   - [Table Management](#table-management)
   - [Writing Data](#writing-data)
   - [Reading Data](#reading-data)
   - [Joins](#joins)
   - [Aggregation & GROUP BY](#aggregation--group-by)
   - [Fuzzy Search](#fuzzy-search)
   - [Vector Similarity Search](#vector-similarity-search)
   - [Streaming Watch Queries](#streaming-watch-queries)
   - [Transactions](#transactions)
   - [REST API](#rest-api)
   - [Security & User Management](#security--user-management)
   - [Cluster Commands](#cluster-commands)
   - [Admin Commands](#admin-commands)
   - [Resource Configuration](#resource-configuration)
6. [Expressions & Operators](#expressions--operators)
7. [Response Format](#response-format)
8. [Metrics](#metrics)
9. [Architecture](#architecture)
10. [Quick Reference Card](#quick-reference-card)
11. [Troubleshooting](#troubleshooting)
12. [License & Pricing](#license--pricing)

---

## Installation

### Option A — MSI Installer (Windows)

1. Download `flowdb-0.2.0-x86_64.msi` from the [releases page](https://github.com/Ajaikumar0712/Flow_DB/releases)
2. Double-click and follow the installer wizard
3. Accept the BUSL-1.1 license agreement
4. Keep "Add to PATH" checked (recommended)
5. Click **Install**

**Installed files:**

| File | Location |
|---|---|
| `flowdb-server.exe` | `C:\Program Files\FlowDB\bin\` |
| `flowdb-repl.exe` | `C:\Program Files\FlowDB\bin\` |
| `LICENSE` | `C:\Program Files\FlowDB\` |
| `FlowDB_Documentation.docx` | `C:\Program Files\FlowDB\` |

To uninstall: **Settings → Apps → FlowDB → Uninstall**

---

### Option B — Build from Source

**Requirements:** Rust 1.70 or newer — install from [rustup.rs](https://rustup.rs)

```powershell
git clone https://github.com/Ajaikumar0712/Flow_DB
cd Flow_DB

# Release build (optimised, recommended)
cargo build --release

# Run the test suite (104 tests)
cargo test
```

Output binaries:

| Binary | Path |
|---|---|
| `flowdb-server.exe` | `target\release\flowdb-server.exe` |
| `flowdb-repl.exe` | `target\release\flowdb-repl.exe` |

---

## Running the Server

Start FlowDB in the foreground. Logs print to the terminal. Press `Ctrl+C` to stop.

```powershell
flowdb-server
```

Default address: `127.0.0.1:7878`

### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--addr <HOST:PORT>` | `127.0.0.1:7878` | Address and port to listen on |
| `--wal <PATH>` | `flowdb.wal` | Path to the Write-Ahead Log file |
| `--log-level <LEVEL>` | `info` | Verbosity: `trace`, `debug`, `info`, `warn`, `error` |

**Examples:**

```powershell
# Listen on all network interfaces
flowdb-server --addr 0.0.0.0:7878

# Custom WAL file location
flowdb-server --wal C:\data\flowdb.wal

# Debug logging
flowdb-server --log-level debug

# All flags combined
flowdb-server --addr 0.0.0.0:7878 --wal C:\data\flowdb.wal --log-level info
```

---

## Windows Service

The Windows Service lets FlowDB run automatically in the background — it starts when Windows boots, with no terminal required.

> **All service commands require an Administrator terminal.**
> Right-click PowerShell → "Run as Administrator"

### Setup Workflow

```powershell
# 1. Register the service (only needed once)
flowdb-server install

# 2. Start it
flowdb-server start

# 3. Connect from any terminal (no admin needed)
flowdb-repl

# 4. Stop when finished
flowdb-server stop

# 5. Remove the service registration
flowdb-server uninstall
```

You can also manage it from the Windows Services panel:
`Win+R → services.msc → "FlowDB Database"`

### Install with Custom Settings

The address and WAL path you specify at install time are baked into the service:

```powershell
flowdb-server --addr 0.0.0.0:7878 --wal C:\data\flowdb.wal install
```

### Service Command Reference

| Command | Description | Requires Admin |
|---------|-------------|:--------------:|
| `flowdb-server install` | Register as a Windows Service | ✅ |
| `flowdb-server uninstall` | Remove the service registration | ✅ |
| `flowdb-server start` | Start the service | ✅ |
| `flowdb-server stop` | Stop the service | ✅ |

---

## Connecting with the REPL

`flowdb-repl` is the interactive command-line client.

```powershell
# Connect to the local server (default: 127.0.0.1:7878)
flowdb-repl

# Connect to a remote server
flowdb-repl --addr 192.168.1.10:7878
```

At the `flowql>` prompt, type any FlowQL statement and press Enter to execute it.

| Input | Action |
|-------|--------|
| Any FlowQL statement | Execute and print result |
| `help` or `\help` | Show help |
| `exit`, `quit`, or `\q` | Disconnect and exit |
| `Ctrl+C` / `Ctrl+D` | Disconnect and exit |

---

## FlowQL Language Reference

FlowQL is FlowDB's custom query language. It is **not SQL** — it uses different keywords designed to be readable and unambiguous.

- Statements can be separated by semicolons (`;`)
- Identifiers are case-insensitive
- String values use double quotes: `"hello"`
- Passwords use single quotes: `'secret'`

---

### Data Types

| Type | Description | Example |
|------|-------------|---------|
| `int` | 64-bit signed integer | `42`, `-10` |
| `float` | 64-bit floating point | `3.14`, `-0.5` |
| `text` | UTF-8 string | `"hello world"` |
| `bool` | Boolean | `true`, `false` |
| `json` | Any JSON value | `{"key": "val"}` |
| `blob` | Raw binary data | — |
| `vector` | Dense f32 vector for similarity search | `[0.1, 0.9, 0.3]` |
| `null` | Absent / null value | `null` |

---

### Table Management

#### MAKE TABLE — Create a table

```
MAKE TABLE <table> (
    <column> <type> [PRIMARY KEY],
    ...
)
```

```sql
MAKE TABLE users (
    id     int  PRIMARY KEY,
    name   text,
    age    int,
    active bool
)

MAKE TABLE products (
    id    int   PRIMARY KEY,
    name  text,
    price float,
    tags  json
)
```

#### MAKE INDEX — Create an index

Indexes speed up `WHERE` and `ORDER BY` on frequently-queried columns.

```sql
MAKE INDEX ON users (name)
MAKE INDEX ON products (price)
```

#### DROP TABLE — Delete a table and all its data

```sql
DROP TABLE users
```

#### SHOW TABLES — List all tables

```sql
SHOW TABLES
```

---

### Writing Data

#### PUT — Insert or replace a row

```
PUT <table> { <field>: <value>, ... }
```

If a row with the same primary key already exists, `PUT` replaces it (upsert).

```sql
PUT users { id: 1, name: "Alice", age: 30, active: true }
PUT users { id: 2, name: "Bob",   age: 25, active: false }
PUT users { id: 3, name: "Carol", age: 35, active: true }
```

#### SET — Update existing rows

```
SET <table> { <field>: <value>, ... } [WHERE <condition>]
```

```sql
-- Update one row
SET users { age: 31 } WHERE id = 1

-- Update all matching rows
SET users { active: false } WHERE age < 25

-- Update every row (no WHERE clause)
SET products { price: 0.0 }
```

#### DEL — Delete rows

```
DEL <table> [WHERE <condition>]
```

```sql
-- Delete one row
DEL users WHERE id = 2

-- Delete matching rows
DEL users WHERE active = false

-- Clear the entire table
DEL users
```

---

### Reading Data

#### GET — Retrieve rows

```
GET <table>
    [JOIN ...]
    [WHERE <condition>]
    [GROUP BY ...]
    [ORDER BY <column> [ASC | DESC]]
    [LIMIT <n>]
    [TIMEOUT "<duration>"]
```

```sql
-- All rows
GET users

-- Filtered
GET users WHERE active = true
GET users WHERE age >= 25

-- Compound condition
GET users WHERE age >= 25 AND active = true

-- Ordered and limited
GET users ORDER BY age DESC LIMIT 10

-- With query timeout
GET users WHERE active = true TIMEOUT "5s"
```

**TIMEOUT duration formats:**

| Format | Meaning |
|--------|---------|
| `"500ms"` | 500 milliseconds |
| `"5s"` | 5 seconds |
| `"2m"` | 2 minutes |

> The duration must be a **quoted string**: `TIMEOUT "5s"` ✅ — not `TIMEOUT 5s` ❌

---

### Joins

Combine rows from two tables in a single query.

```
GET <left_table>
    [INNER | LEFT | RIGHT] JOIN <right_table> ON <condition>
    [WHERE <condition>]
    [ORDER BY <col>]
    [LIMIT <n>]
```

| Join Type | Returns |
|-----------|---------|
| `INNER JOIN` | Only rows that match in **both** tables |
| `LEFT JOIN` | **All** left rows; `null` for unmatched right columns |
| `RIGHT JOIN` | **All** right rows; `null` for unmatched left columns |

**Setup:**

```sql
MAKE TABLE users  (id int PRIMARY KEY, name text)
MAKE TABLE orders (id int PRIMARY KEY, user_id int, total float)

PUT users  { id: 1, name: "Alice" }
PUT users  { id: 2, name: "Bob" }
PUT orders { id: 10, user_id: 1, total: 49.99 }
PUT orders { id: 11, user_id: 1, total: 12.50 }
```

**Queries:**

```sql
-- Only users who have placed orders
GET users INNER JOIN orders ON users.id = orders.user_id

-- All users, including those with no orders
GET users LEFT JOIN orders ON users.id = orders.user_id

-- Filtered + sorted join
GET users INNER JOIN orders ON users.id = orders.user_id
    WHERE orders.total > 20.0
    ORDER BY orders.total DESC
    LIMIT 5
```

> When both tables share a column name, the right-table column is prefixed with the table name (e.g. `orders.id`).

---

### Aggregation & GROUP BY

Group rows and calculate aggregate values. Use `HAVING` to filter grouped results.

```
GET <table>
    [WHERE <condition>]
    GROUP BY <col> [, <col>]
        [COUNT(*) | SUM(<col>) | AVG(<col>) | MIN(<col>) | MAX(<col>)] [AS <alias>]
    [HAVING <condition>]
    [ORDER BY <col>]
    [LIMIT <n>]
```

**Aggregate functions:**

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows in the group |
| `COUNT(<col>)` | Count non-null values in a column |
| `SUM(<col>)` | Sum of all values |
| `AVG(<col>)` | Arithmetic mean |
| `MIN(<col>)` | Minimum value |
| `MAX(<col>)` | Maximum value |

**Examples:**

```sql
MAKE TABLE orders (id int PRIMARY KEY, user_id int, country text, total float)

PUT orders { id: 1, user_id: 1, country: "US", total: 49.99 }
PUT orders { id: 2, user_id: 2, country: "US", total: 12.50 }
PUT orders { id: 3, user_id: 3, country: "UK", total: 30.00 }

-- Count orders per country
GET orders GROUP BY country COUNT(*) AS order_count

-- Total revenue per country, only where more than 1 order placed
GET orders GROUP BY country SUM(total) AS revenue HAVING order_count > 1

-- Average order value per user, best first
GET orders GROUP BY user_id AVG(total) AS avg_total ORDER BY avg_total DESC
```

---

### Fuzzy Search

#### FIND — Trigram-based similarity search

Find rows where a text column closely resembles a search pattern. Handles typos and partial matches.

```
FIND <table> WHERE <column> ~ "<pattern>" [LIMIT <n>]
```

Results are ranked by similarity score (best match first).

```sql
-- Finds "Alice" even with typo "alic"
FIND users WHERE name ~ "alic"

-- Partial match — "widge" finds "Widget"
FIND products WHERE name ~ "widge" LIMIT 5
```

The `~` operator can also be used inside a regular `GET`:

```sql
GET users WHERE name ~ "alice"
```

---

### Vector Similarity Search

Store dense float vectors in a `vector` column and search for the closest rows by cosine similarity. Results are ranked by an `_score` column (0.0–1.0). Backed by an **HNSW** index for fast approximate nearest-neighbour lookup.

```
SIMILAR <table> [ON <column>] TO [<f>, <f>, ...] [LIMIT <n>]
```

**Setup:**

```sql
MAKE TABLE items (
    id        int    PRIMARY KEY,
    label     text,
    embedding vector
)

PUT items { id: 1, label: "cat",  embedding: [0.9, 0.1, 0.0] }
PUT items { id: 2, label: "dog",  embedding: [0.8, 0.2, 0.1] }
PUT items { id: 3, label: "fish", embedding: [0.1, 0.1, 0.9] }
```

**Query:**

```sql
SIMILAR items ON embedding TO [0.85, 0.15, 0.05] LIMIT 2
```

**Response:**

```json
{
  "status": "ok",
  "result": {
    "Rows": {
      "columns": ["id", "label", "embedding", "_score"],
      "rows": [
        [1, "cat", "vec[3 dims]", 0.9998],
        [2, "dog", "vec[3 dims]", 0.9965]
      ]
    }
  }
}
```

> If the table has only one `vector` column, `ON <column>` can be omitted.

---

### Streaming Watch Queries

`WATCH` opens a **server-push subscription**. The server streams a JSON event to your connection every time a matching row is inserted, updated, or deleted — no polling required.

#### WATCH — Subscribe to changes

```
WATCH <table> [WHERE <condition>]
```

```sql
WATCH users WHERE active = true
```

**Acknowledgement (sent once immediately):**

```json
{"status":"ok","watch_id":1,"message":"watching, subscription id=1"}
```

**Push events** (one JSON line per change, streamed in real time):

```json
{"status":"watch","id":1,"op":"insert","row":{"id":4,"name":"Dave","age":22,"active":true}}
{"status":"watch","id":1,"op":"update","row":{"id":2,"name":"Bob","age":26,"active":true}}
{"status":"watch","id":1,"op":"delete","row":{"id":3,"name":"Carol","age":35,"active":true}}
```

| `op` | Triggered by |
|------|-------------|
| `insert` | `PUT` added a row matching the filter |
| `update` | `SET` changed a row matching the filter |
| `delete` | `DEL` removed a row that matched the filter |

You can have multiple active subscriptions per connection — each gets a unique `watch_id`. Other queries can still run on the same connection while watching.

#### UNWATCH — Cancel a subscription

```sql
UNWATCH 1
```

---

### Transactions

Operations inside a transaction are applied atomically — either **all succeed** or **all are rolled back**.

```sql
BEGIN

PUT users { id: 10, name: "Dave", age: 28, active: true }
SET users { age: 29 } WHERE id = 10
DEL users WHERE id = 5

COMMIT
```

**Rollback:**

```sql
BEGIN
PUT users { id: 99, name: "Test", age: 0 }
ROLLBACK
```

| Statement | Description |
|-----------|-------------|
| `BEGIN` | Start a transaction |
| `COMMIT` | Apply all changes permanently |
| `ROLLBACK` | Discard all changes since `BEGIN` |

FlowDB uses **MVCC (Multi-Version Concurrency Control)** — readers never block writers and writers never block readers.

---

### REST API

FlowDB can expose any table as an HTTP REST endpoint — useful for integrating with tools or services that speak HTTP instead of FlowQL.

#### API GENERATE — Create an HTTP endpoint for a table

```
API GENERATE <table>
```

```sql
API GENERATE users
```

This starts an HTTP server on a local port and logs the address. The endpoint supports:

| HTTP Method | Action |
|-------------|--------|
| `GET /` | Fetch all rows (equivalent to `GET <table>`) |
| `POST /` | Insert a row (request body: JSON object) |

**Example (PowerShell):**

```powershell
# Read all rows
Invoke-RestMethod -Uri "http://127.0.0.1:<port>/" -Method GET

# Insert a row
Invoke-RestMethod -Uri "http://127.0.0.1:<port>/" -Method POST `
    -ContentType "application/json" `
    -Body '{"id":99,"name":"REST User","age":22}'
```

---

### Security & User Management

FlowDB supports user accounts with SHA-256 hashed passwords, admin roles, and per-table operation grants.

By default the server starts in **open mode** — no authentication required. When secured mode is enabled, every connection must call `AUTH` before running queries.

#### AUTH — Authenticate a session

```sql
AUTH alice 'secret123'
```

#### CREATE USER — Add a user account

```sql
-- Regular user
CREATE USER bob PASSWORD 'hunter2'

-- Admin user (can manage users and grants)
CREATE ADMIN USER carol PASSWORD 'admin-pass'
```

#### DROP USER — Remove a user

```sql
DROP USER bob
```

#### GRANT — Give a user permission on a table

```
GRANT <op> ON <table | *> TO <username>
```

Operations: `select`, `insert`, `update`, `delete`, `all`

```sql
-- Allow bob to read from users
GRANT select ON users TO bob

-- Give carol full access to all tables
GRANT all ON * TO carol
```

#### REVOKE — Remove a permission

```sql
REVOKE insert ON orders FROM bob
```

#### SHOW USERS — List all accounts

```sql
SHOW USERS
```

| Column | Description |
|--------|-------------|
| `username` | Account name |
| `is_admin` | `true` if the account has admin privileges |

---

### Cluster Commands

FlowDB supports a lightweight cluster layer with TCP heartbeat monitoring, Raft-based leader election, and hash-based shard routing.

> **Note:** Data replication between nodes is not yet implemented. The cluster layer currently handles membership tracking and health monitoring only.

#### CLUSTER JOIN — Add a peer node

```sql
CLUSTER JOIN "192.168.1.20:7878"
```

#### CLUSTER PART — Remove a peer node

```sql
CLUSTER PART "192.168.1.20:7878"
```

#### CLUSTER STATUS — List all peers and their health

```sql
CLUSTER STATUS
```

| Column | Description |
|--------|-------------|
| `addr` | Peer address (`host:port`) |
| `reachable` | `true` if the last heartbeat succeeded |
| `latency_ms` | Round-trip time in ms, or `null` if never probed |

#### CLUSTER SHARD ASSIGN — Configure hash sharding for a table

Distributes a table across N shards using FNV-1a hash routing, assigned round-robin to the listed nodes.

```sql
CLUSTER SHARD ASSIGN orders SHARDS 4 NODES "node1:7878", "node2:7878", "node3:7878"
```

#### CLUSTER SHARD STATUS — Show all shard assignments

```sql
CLUSTER SHARD STATUS
```

#### CLUSTER SHARD DROP — Remove shard assignment for a table

```sql
CLUSTER SHARD DROP orders
```

---

### Admin Commands

#### EXPLAIN — Preview the execution plan

Prefix any query with `EXPLAIN` to see how FlowDB will execute it, without running it. Useful for understanding index usage and optimiser decisions.

```sql
EXPLAIN GET users WHERE age > 25 ORDER BY age DESC LIMIT 10
EXPLAIN PUT users { id: 999, name: "Test" }
EXPLAIN FIND products WHERE name ~ "widget"
```

#### CHECKPOINT — Flush all data to disk immediately

```sql
CHECKPOINT
```

#### SHOW RUNNING QUERIES — List active queries

```sql
SHOW RUNNING QUERIES
```

Returns each executing query with its ID, text, start time, and elapsed time.

#### KILL QUERY — Stop a running query

```sql
-- Find the query ID first
SHOW RUNNING QUERIES

-- Kill it by ID
KILL QUERY 42
```

#### METRICS — Server statistics

```sql
METRICS
```

See the [Metrics](#metrics) section for sample output.

---

### Resource Configuration

Adjust runtime limits without restarting the server.

#### CONFIG SET — Set a resource limit

```
CONFIG SET <key> <value>
```

| Key | Type | Description |
|-----|------|-------------|
| `max_connections` | int | Maximum concurrent client connections |
| `max_rows_per_query` | int | Maximum rows a single query can return |
| `default_timeout_ms` | int | Default query timeout in milliseconds |
| `max_memory_mb` | int | Soft memory usage cap in MB |
| `disk_quota_mb` | int | Disk usage cap in MB |

```sql
CONFIG SET max_connections 200
CONFIG SET max_rows_per_query 50000
CONFIG SET default_timeout_ms 10000
```

#### SHOW CONFIG — Display current limits

```sql
SHOW CONFIG
```

---

## Expressions & Operators

Used in `WHERE` conditions and `SET` values.

### Comparison

| Operator | Meaning |
|----------|---------|
| `=` | Equal |
| `!=` or `<>` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |
| `~` | Fuzzy text match (trigram similarity) |

### Logical

| Operator | Meaning |
|----------|---------|
| `AND` | Both conditions must be true |
| `OR` | Either condition must be true |
| `NOT` | Negation |

### Arithmetic

| Operator | Meaning |
|----------|---------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |

**Examples:**

```sql
-- Range condition
GET users WHERE age >= 18 AND age <= 65 AND active = true

-- OR
GET users WHERE name = "Alice" OR name = "Bob"

-- NOT
GET users WHERE NOT active = true

-- Arithmetic in filter
GET products WHERE price * 1.2 < 30.0
```

---

## Response Format

All communication between the server and client uses **newline-terminated JSON**.

**Success — rows returned:**

```json
{
  "status": "ok",
  "result": {
    "Rows": {
      "columns": ["id", "name", "age"],
      "rows": [
        [1, "Alice", 30],
        [2, "Bob",   25]
      ]
    }
  }
}
```

**Success — write operation:**

```json
{"status":"ok","result":{"Ok":{"message":"1 row inserted","elapsed_ms":0}}}
```

**Error:**

```json
{"status":"error","message":"table 'users' does not exist"}
```

---

## Metrics

After running some queries, `METRICS` returns a snapshot like this:

```
=== FlowDB Metrics ===
Uptime:                    5m 32s
Queries total:             47
Queries errored:           1
Rows inserted:             20
Rows updated:              3
Rows deleted:              2
Transactions committed:    4
Transactions rolled back:  0
WAL records written:       35

Latency (ms):
  Min:   0
  Avg:   1.2
  P95:   4
  P99:   9
  Max:   12
```

---

## Architecture

![FlowDB System Architecture](docs/architecture.png)

```mermaid
flowchart TD
    subgraph CLIENTS["CLIENT APPLICATIONS"]
        direction LR
        CLI["`**flowdb-repl**
        CLI`"]
        REST["`**REST API**
        clients`"]
        CUSTOM["`**Custom**
        applications`"]
    end

    CLIENTS -->|TCP / HTTP communication| SERVER

    subgraph SERVER["🗄️  FlowDB SERVER"]
        direction TB

        subgraph QE["QUERY ENGINE"]
            direction LR
            P[Parser] --> PL[Planner] --> EX[Executor]
        end

        subgraph TL["TRANSACTION LAYER  —  MVCC + WAL"]
        end

        subgraph SE["STORAGE ENGINE"]
            direction LR
            MEM[In-Memory Tables] --- IDX[Indexes]
        end

        subgraph BG["BACKGROUND SERVICES"]
            direction LR
            WAL[WAL Writer]
            SNAP[Snapshot Manager]
            WATCH[Streaming Watch Engine]
            RAFT[Cluster Manager Raft]
        end

        QE --> TL --> SE
        SE -.-> BG
    end
```

### Layer Breakdown

| Layer | Components | Responsibility |
|-------|-----------|---------------|
| **Query Engine** | `lexer` → `parser` → `planner` → `executor` | Parse FlowQL text, build AST, cost-estimate execution plan, run it |
| **Transaction Layer** | `transaction.rs`, `mvcc.rs`, `wal.rs` | MVCC snapshot isolation, BEGIN/COMMIT/ROLLBACK, Write-Ahead Log |
| **Storage Engine** | `table.rs`, `columnar.rs`, `buffer_pool.rs`, `lsm.rs` | In-memory B-tree tables, columnar compression, LRU page cache, LSM compaction |
| **Background Services** | `wal.rs`, `persist.rs`, `watch.rs`, `cluster/` | WAL flushing, disk snapshots, streaming push events, Raft consensus |

### Source Tree

```
flowdb-server
  ├── server.rs          — TCP listener; channel-based writer per connection
  ├── auth.rs            — Users, SHA-256 passwords, roles, per-table grants
  ├── resource.rs        — Connection limits, row quotas, query deadlines
  ├── sql/
  │   ├── lexer.rs       — Tokeniser
  │   ├── parser.rs      — Recursive-descent parser → AST
  │   └── ast.rs         — Statement & expression types
  ├── engine/
  │   ├── executor.rs    — AST → storage ops; JOIN, GROUP BY, SIMILAR, auth dispatch
  │   ├── evaluator.rs   — WHERE filter evaluation, trigram fuzzy search
  │   ├── planner.rs     — Histogram-based cost planner (NDV + equi-depth buckets)
  │   ├── hnsw.rs        — HNSW Approximate Nearest Neighbour index
  │   └── watch.rs       — WatchRegistry: subscribe / unsubscribe / notify
  ├── cluster/
  │   ├── mod.rs         — ClusterRegistry: join/part/status/heartbeat tracking
  │   ├── raft.rs        — Raft consensus: leader election + log replication
  │   └── shard.rs       — ShardManager: FNV-1a hash routing
  ├── storage/
  │   ├── table.rs       — In-memory table store, B-tree indexes, histograms
  │   ├── columnar.rs    — Dictionary / RLE / bitmap column compression
  │   ├── persist.rs     — Disk catalog + snapshot save/recover; CHECKPOINT
  │   ├── buffer_pool.rs — LRU page cache (8 KB pages)
  │   └── lsm.rs         — MemTable → SSTable → compaction pipeline
  ├── mvcc.rs            — Snapshot isolation (xmin/xmax per row)
  ├── transaction.rs     — BEGIN/COMMIT/ROLLBACK with WAL integration
  ├── wal.rs             — Append-only Write-Ahead Log
  ├── metrics.rs         — Atomic counters + latency histogram
  └── error.rs           — Unified error type
```

### WAL (Write-Ahead Log)

Every mutation (`PUT`, `SET`, `DEL`) is written to `flowdb.wal` before being applied in memory. On restart, the WAL is replayed to restore the last committed state.

### Disk Snapshots

In addition to the WAL, FlowDB periodically writes full data snapshots to the `--data-dir` directory. On startup, snapshots are loaded first, then the WAL tail is replayed on top.

### WATCH Streaming

Each `WATCH` subscriber gets a dedicated Tokio channel. When any mutation reaches the executor it calls `WatchRegistry::notify()`, which evaluates each subscriber's optional filter and sends matching events only. A background task per subscription drains the channel and writes event lines to the TCP connection — so the read loop and all watch tasks write concurrently without blocking each other.

### Columnar Compression

| Encoding | Applied when |
|----------|-------------|
| **Bitmap** | Boolean columns |
| **Dictionary** | Low-cardinality text (≤ 256 distinct values per batch) |
| **RLE** | Long runs of repeated values |
| **Raw** | Everything else |

### Query Planner

Uses **equi-depth histograms** (16 buckets per column) plus **Number of Distinct Values (NDV)** to estimate filter selectivity. Range predicates use fractional bucket coverage rather than a flat 1/NDV estimate. If an index scan is estimated to touch more rows than a full scan, the planner falls back to a full scan automatically.

---

## Quick Reference Card

```sql
-- ── Tables ────────────────────────────────────────────────────────────
MAKE TABLE t (id int PRIMARY KEY, name text, val float, emb vector)
MAKE INDEX ON t (name)
DROP TABLE t
SHOW TABLES

-- ── Write ─────────────────────────────────────────────────────────────
PUT t { id: 1, name: "Alice", val: 3.14, emb: [0.9, 0.1, 0.0] }
SET t { val: 9.99 } WHERE id = 1
DEL t WHERE id = 1

-- ── Read ──────────────────────────────────────────────────────────────
GET t
GET t WHERE val > 5.0
GET t WHERE name = "Alice" AND val > 0
GET t ORDER BY val DESC LIMIT 10
GET t TIMEOUT "5s"

-- ── Fuzzy text search ─────────────────────────────────────────────────
FIND t WHERE name ~ "alic"

-- ── Vector similarity ─────────────────────────────────────────────────
SIMILAR t ON emb TO [0.85, 0.15, 0.05] LIMIT 5

-- ── Streaming subscriptions ───────────────────────────────────────────
WATCH t WHERE val > 5.0          -- streams events until unwatch
UNWATCH 1                         -- cancel subscription id=1

-- ── Transactions ──────────────────────────────────────────────────────
BEGIN
  PUT t { id: 10, name: "Test" }
  SET t { val: 1.0 } WHERE id = 10
COMMIT

ROLLBACK

-- ── Joins ─────────────────────────────────────────────────────────────
GET users INNER JOIN orders ON users.id = orders.user_id
GET users LEFT  JOIN orders ON users.id = orders.user_id WHERE orders.total > 10
GET users RIGHT JOIN orders ON users.id = orders.user_id LIMIT 20

-- ── Aggregation ───────────────────────────────────────────────────────
GET orders GROUP BY country COUNT(*) AS cnt SUM(total) AS revenue
GET orders GROUP BY user_id AVG(total) AS avg ORDER BY avg DESC

-- ── REST API ──────────────────────────────────────────────────────────
API GENERATE users

-- ── Security ──────────────────────────────────────────────────────────
AUTH alice 'secret123'
CREATE USER bob PASSWORD 'pass'
CREATE ADMIN USER carol PASSWORD 'admin'
GRANT select ON users TO bob
GRANT all ON * TO carol
REVOKE insert ON orders FROM bob
DROP USER bob
SHOW USERS

-- ── Cluster ───────────────────────────────────────────────────────────
CLUSTER JOIN "192.168.1.20:7878"
CLUSTER PART "192.168.1.20:7878"
CLUSTER STATUS
CLUSTER SHARD ASSIGN orders SHARDS 4 NODES "n1:7878", "n2:7878"
CLUSTER SHARD STATUS
CLUSTER SHARD DROP orders

-- ── Resource limits ───────────────────────────────────────────────────
CONFIG SET max_connections 200
CONFIG SET default_timeout_ms 10000
SHOW CONFIG

-- ── Admin ─────────────────────────────────────────────────────────────
EXPLAIN GET t WHERE val > 5
CHECKPOINT
METRICS
SHOW TABLES
SHOW RUNNING QUERIES
KILL QUERY 7
```

---

## Troubleshooting

### "address already in use" — server won't start

Another process is using port 7878. Stop that process, or start FlowDB on a different port:

```powershell
flowdb-server --addr 127.0.0.1:7879
```

### "connection refused" — REPL can't connect

The server is not running. Start it first with `flowdb-server`, then open the REPL.

### "table does not exist" error

The table hasn't been created yet. Run `MAKE TABLE` first, then use `PUT`/`GET`.

### PUT silently replaces an existing row

`PUT` is an upsert by design — if a row with the same primary key exists, it is replaced. Use `SET` to update specific fields without replacing the entire row.

### Query timeout errors

Increase the per-query timeout or the server-wide default:

```sql
-- Per-query timeout
GET big_table LIMIT 1000 TIMEOUT "30s"

-- Server-wide default
CONFIG SET default_timeout_ms 30000
```

### Windows Service — "access denied"

Service commands require an **Administrator** terminal. Right-click PowerShell and choose "Run as Administrator".

### WAL file growing very large

Run a manual checkpoint to flush a full snapshot to disk and truncate the WAL:

```sql
CHECKPOINT
```

---

## License & Pricing

FlowDB is licensed under the **Business Source License 1.1 (BUSL-1.1)**.

| Use case | Allowed |
|---|:-:|
| Personal use, learning, experimentation | ✅ Free |
| Open-source non-commercial projects | ✅ Free |
| **30-day production trial** | ✅ Free |
| Commercial production use (after trial) | ❌ Paid license required |

On **March 11, 2030**, FlowDB automatically converts to the **Apache License 2.0** and becomes free for everyone, forever.

### Commercial Licensing

| Tier | Price | Includes |
|---|---|---|
| **Starter** | $500 / year | 1 server, 1 company |
| **Business** | $2,000 / year | Up to 5 servers |
| **Enterprise** | Contact us | Unlimited servers, priority support, SLA |

**To purchase or enquire:**

- **Email:** flowdb.license@gmail.com
- **Website:** [https://fornexus.tech](https://fornexus.tech)

See the full [LICENSE](LICENSE) file for complete terms.

---

*FlowDB is a product of [ForNexus](https://fornexus.tech), developed by Ajaikumar.*
