# Flat-DB

A minimal but real storage engine implemented in Python, built as Part 2 of a DBMS course project.

---

## Table of Contents

1. [Chosen Track & Rationale](#1-chosen-track--rationale)
2. [On-Disk Layout](#2-on-disk-layout)
3. [RID Semantics](#3-rid-semantics)
4. [Restart / Recovery](#4-restart--recovery)
5. [Complexity Notes](#5-complexity-notes)
6. [API Reference](#6-api-reference)
7. [Compression (Advanced)](#7-compression-advanced)
8. [Installation & Usage](#8-installation--usage)
9. [Tests](#9-tests)
10. [Demo Script](#10-demo-script)
11. [CLI Usage](#11-cli-usage)

---

## 1. Chosen Track & Rationale

**Track A – OLTP Row-Store** with slotted heap pages.

**Workload assumptions:**
- Frequent point reads and writes by primary key / RID.
- Row-at-a-time inserts and updates (not bulk-analytical).
- Variable-length TEXT fields are common.
- Tables small enough to fit on a laptop for a course demo.

A **heap file** with slotted pages is the simplest production-realistic design that satisfies all requirements: it supports variable-length records naturally, provides O(1) point operations once the RID is known, and is well understood in the literature (PostgreSQL's heap uses the same principle).

---

## 2. On-Disk Layout

### Files

```
<data_dir>/
  catalog.json        ← schema + free-space map for all tables (JSON, atomic writes)
  <table_name>.db     ← heap file – a flat sequence of 4 KB pages
  <table_name>.db     ← (one file per table)
```

### Page layout  (`PAGE_SIZE = 4096` bytes)

```
┌─────────────────────────────────────────────────────┐  ← offset 0
│  Header  (16 bytes)                                 │
├─────────────────────────────────────────────────────┤  ← offset 16
│                                                     │
│  Record data  →  (fss advances right on insert)    │
│                                                     │
│           ···  free space  ···                      │
│                                                     │
│  ← Slot directory  (fse retreats left on new slot)  │
├─────────────────────────────────────────────────────┤  ← offset 4092 (last slot)
│  [slot 0]  [slot 1]  …                              │
└─────────────────────────────────────────────────────┘  ← offset 4095
```

#### Header (16 bytes, big-endian)

| Field      | Type    | Size | Description |
|------------|---------|------|-------------|
| `page_id`  | uint32  | 4 B  | Persistent page number (0-based) |
| `num_slots`| uint16  | 2 B  | Total slot entries allocated (live + deleted) |
| `fss`      | uint16  | 2 B  | `free_space_start`: next byte to write a record |
| `fse`      | uint16  | 2 B  | `free_space_end`: first byte of the slot directory |
| `flags`    | uint16  | 2 B  | Reserved |
| `reserved` | uint32  | 4 B  | Reserved |

**Free bytes** on a page = `fse – fss`.

#### Slot entry (4 bytes each, growing from the *end* of the page backward)

| Field    | Type   | Size | Description |
|----------|--------|------|-------------|
| `offset` | uint16 | 2 B  | Byte offset of the record within this page |
| `length` | uint16 | 2 B  | Byte length of the record (bit 15 = MOVED_FLAG) |

**Slot sentinel values:**

| `offset`  | `length` | Meaning |
|-----------|----------|---------|
| `0x0000`  | `0x0000` | Deleted tombstone |
| `0xFFFE`  | `off`    | Forwarding pointer; 8-byte target RID at byte `off` within the page |
| any       | `\| 0x8000` | MOVED_FLAG: record was relocated here by an overflow update; real length = `length & 0x7FFF`; scanner skips this slot |

#### Record encoding (variable-length, big-endian)

```
[null_bitmap: ⌈ncols/8⌉ bytes]
For each column i, if NOT null:
  INT    → 4-byte signed integer
  BIGINT → 8-byte signed integer
  FLOAT  → 8-byte IEEE 754 double
  TEXT   → uint16 length prefix (2 B) + UTF-8 bytes
  BOOL   → 1 byte (0 or 1)
```

All records are zero-padded to a minimum of 8 bytes so that forwarding pointers can always be written in-place.

#### `catalog.json` structure

```json
{
  "tables": {
    "users": {
      "schema": [
        {"name": "id",   "type": "INT"},
        {"name": "name", "type": "TEXT"}
      ],
      "page_count": 3,
      "fsm": [120, 4040, 3900]
    }
  }
}
```

`fsm[i]` = free bytes remaining on page `i` = what the engine consults when looking for a page with enough space to insert a new record.

---

## 3. RID Semantics

```
RID = (page_id: int, slot_id: int)
```

- Both components are **0-based**.
- A RID is **stable** once issued by `insert()`.
- If an `update()` call produces a record larger than the old slot, the old slot is converted to a **forwarding pointer** and a new slot is allocated (possibly on a different page). The caller's RID continues to resolve to the correct data; `get()` / `update()` / `delete()` follow the chain transparently.
- `scan()` yields every live record exactly once, using the **original** RID as the identifier (it follows forwarding pointers for moved records and skips MOVED_FLAG slots that are reachable only through a forward chain).
- Free-list: deleted slots (offset=0, length=0) are reused by the next `write_record()` call on that page, saving slot-directory space.

---

## 4. Restart / Recovery

| Mechanism | Detail |
|-----------|--------|
| **Page writes** | Every `write_page()` call calls `f.flush()` + `os.fsync()`, so pages are durable before the call returns. |
| **Catalog** | Written atomically: new JSON → `catalog.json.tmp`, then `os.replace()` (POSIX atomic rename). A crash mid-write leaves the old catalog intact. |
| **No WAL** | There is no write-ahead log. The engine is crash-safe at single-page granularity; a crash during a multi-page operation (e.g. overflow update that spans two pages) could leave the database in an inconsistent state between the two pages. For course purposes this is acceptable. |
| **On restart** | `StorageEngine(data_dir)` loads `catalog.json` (schema + FSM) and re-opens heap files. No scan or recovery pass is required. |

---

## 5. Complexity Notes

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `insert`  | **O(1)** amortised | FSM lookup in the in-memory catalog; worst case O(P) if every page is full and we must create a new one. Each page write is O(page\_size). |
| `get`     | **O(1)** | Direct seek to page, slot lookup, decode. One extra page read per forwarding hop (chain depth ≤ 16). |
| `update`  | **O(1)** | In-place if new record ≤ old slot size; otherwise O(1) + one `insert`. |
| `delete`  | **O(1)** | Tombstone write (one page read + write). One extra hop for forwarded records. |
| `scan`    | **O(N)** | Sequential page + slot traversal; N = total live records. |
| `compact` | **O(P × S)** | Page-level vacuum; P = pages, S = slots per page. Not called automatically. |

Space efficiency note: deleted records and forwarding-pointer stubs are not immediately reclaimed. `fss` only advances; free space on a page is the gap between `fss` and `fse`. Call `Page.compact()` to create a packed copy, or vacuum at the table level.

---

## 6. API Reference

```python
from storage import StorageEngine, T_INT, T_TEXT, T_FLOAT, T_BOOL, T_BIGINT

engine = StorageEngine("./data")

# DDL
engine.create_table("users", [
    {"name": "id",    "type": "INT"},
    {"name": "name",  "type": "TEXT"},
    {"name": "score", "type": "FLOAT"},
])
engine.drop_table("users")          # optional; removes file too
engine.list_tables()                # -> List[str]
engine.table_exists("users")        # -> bool

# DML
tbl = engine.open_table("users")   # -> TableHandle

rid = tbl.insert({"id": 1, "name": "Alice", "score": 9.5})
rec = tbl.get(rid)                  # -> dict | None
ok  = tbl.update(rid, {...})        # -> bool
ok  = tbl.delete(rid)               # -> bool

for rid, rec in tbl.scan():        # iterator of (RID, dict)
    print(rid, rec)

tbl.count()                         # -> int  (full scan)
```

---

## 7. Compression (Advanced)

`storage.py` ships two column-level compressors for bulk analytics:

### RLE (Run-Length Encoding) – for `INT` / `BIGINT`

```python
from storage import RLEEncoder, T_INT

vals      = [1, 1, 1, 2, 2, 3]
compressed = RLEEncoder.encode(vals, T_INT)   # bytes
decoded    = RLEEncoder.decode(compressed, T_INT)  # [1,1,1,2,2,3]
ratio      = RLEEncoder.ratio(vals)           # compressed / original
```

Wire format: `[uint32 run_count] [run_count × (value, uint32 count)]`

### Dictionary Encoding – for `TEXT`

```python
from storage import DictEncoder

vals      = ['Engineering', 'HR', 'Engineering', 'Marketing']
compressed = DictEncoder.encode(vals)     # bytes
decoded    = DictEncoder.decode(compressed)
ratio      = DictEncoder.ratio(vals)
```

Wire format: `[uint16 dict_size] [dict entries] [uint32 n_values] [n_values × uint16 code]`

### `scan_compressed`

Scan an entire column from a live table and return it compressed:

```python
from storage import scan_compressed, RLEEncoder, T_INT

comp = scan_compressed(tbl, "id")           # RLE bytes (INT column)
vals = RLEEncoder.decode(comp, T_INT)
```

Typical compression ratios observed in the demo:
- Repeated INT column (1000 identical values): **< 0.2 %** of original size.
- Low-cardinality TEXT column (3 unique values × 500 rows): **< 20 %** of original size.

---

## 8. Installation & Usage

```bash
# Python 3.8+ required
pip install -r requirements.txt
```

`requirements.txt`:
```
pytest>=7.0.0
```

No other dependencies are needed for the storage engine itself.

---

## 9. Tests

```bash
pytest test_storage.py -v
```

Test groups:

| Class | What it covers |
|-------|---------------|
| `TestPage` | Page header, slot read/write, delete tombstone, forward pointer, MOVED_FLAG, compact |
| `TestRecordEncoding` | All column types, null bitmap, unicode, large TEXT, missing-key-as-null |
| `TestHeapFile` | Page I/O, file create/read/multi-page, gap detection |
| `TestStorageCRUD` | insert/get/update/delete, overflow update, scan, multi-page |
| `TestPersistence` | Schema + data + deletes + updates survive engine restart |
| `TestEdgeCases` | varlen boundaries, page-full spill, slot reuse, forward chains, tombstones |
| `TestMultiTable` | Independent tables, drop-one-leave-other, separate heap files |
| `TestCompression` | RLE encode/decode, dict encode/decode, ratio, `scan_compressed` |

---

## 10. Demo Script

```bash
python3 demo.py
```

The demo:
1. Creates `employees` and `departments` tables.
2. Inserts records with all column types (including NULL).
3. Demonstrates in-place update.
4. Forces an **overflow update** (large TEXT field) and shows the forwarding-pointer mechanism.
5. Deletes a record and verifies tombstone behaviour.
6. Performs a full scan.
7. **Destroys the engine object** and creates a new one pointing at the same directory, proving persistence.
8. Verifies all RIDs still resolve correctly after restart.
9. Demonstrates RLE + dictionary compression on a `stats` table.

---

## 11. CLI Usage

```bash
# Parse only (no execution)
python3 db_cli.py --query "CREATE TABLE users (id INT, name TEXT)"

# Print AST as JSON
python3 db_cli.py --debug-ast --query "SELECT * FROM users WHERE id > 5"

# Execute against the storage engine
python3 db_cli.py --execute --db-dir ./mydata \
  --query "CREATE TABLE employees (id INT, name TEXT, salary FLOAT)"

python3 db_cli.py --execute --db-dir ./mydata \
  --query "INSERT INTO employees VALUES (1, 'Alice', 95000.0)"

python3 db_cli.py --execute --db-dir ./mydata \
  --query "SELECT * FROM employees"

python3 db_cli.py --execute --db-dir ./mydata \
  --query "SELECT name, salary FROM employees WHERE salary > 50000"

python3 db_cli.py --execute --db-dir ./mydata \
  --query "SCAN employees"

# Drop a table
python3 db_cli.py --execute --db-dir ./mydata \
  --query "DROP TABLE employees"
```

---

## Part 1 — SQL Parser (unchanged)

The original `db_cli.py` SQL parser remains intact:

- **Lexer**: regex-based tokeniser supporting `INT`, `BIGINT`, `FLOAT`, `TEXT`, `BOOL`, `NULL`
- **Parser**: recursive-descent, handles `CREATE TABLE`, `INSERT INTO`, `SELECT … FROM … WHERE … LIMIT`
- **AST nodes**: `CreateTableStmt`, `InsertStmt`, `SelectStmt`, `BinaryOp`, `Literal`, `ColumnRef`

```bash
# Legacy parse-only mode (no --execute flag)
python3 db_cli.py --query "SELECT name FROM users WHERE id = 1"
# → Query parsed successfully.

python3 db_cli.py --debug-ast --query "CREATE TABLE t (x INT, y TEXT)"
# → { "table_name": "t", "columns": [...], "type": "CREATE_TABLE" }
```
