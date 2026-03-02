# Flat-DB — Codebase Exploration Summary

## Project Overview

**Flat-DB** is a minimal but production-realistic DBMS storage engine implemented in Python, built as Part 2 of a DBMS course project. It implements **Track A — OLTP Row-Store** using slotted heap pages modelled after PostgreSQL's physical storage design.

---

## Project Files

| File | Purpose |
|---|---|
| `storage.py` | Core storage engine (~1046 lines) |
| `db_cli.py` | SQL parser + executor CLI |
| `test_storage.py` | pytest test suite for the storage engine |
| `test-db-cli.py` | unittest test suite for the SQL parser |
| `demo.py` | End-to-end demonstration script |
| `README.md` | User-facing documentation |
| `architecture_part_2.md` | Detailed architecture design document |
| `requirements.txt` | `pytest>=7.0.0` only dependency |

---

## Architecture

The system is two layers stacked on top of each other:

```
db_cli.py   (SQL text → AST → Executor → StorageEngine API)
     │
storage.py  (StorageEngine → Catalog + TableHandle → HeapFile → Page)
     │
  data/      (catalog.json  +  <table>.db  binary heap files)
```

### Part 1 — SQL Parser (`db_cli.py`)

A standalone regex-based lexer and recursive-descent parser that tokenises SQL strings and produces an AST, then dispatches that AST to the storage engine.

**Lexer** — recognises `NUMBER`, `STRING`, `KEYWORD`, `ID`, `OP`, `DELIM` tokens via `re.finditer`.

**Parser** — recursive-descent, supports:
- `CREATE TABLE name (col type, ...)`
- `DROP TABLE name`
- `INSERT INTO name VALUES (...)`
- `SELECT [* | cols] FROM table [WHERE expr] [LIMIT n]`
- `SCAN table` (shorthand for `SELECT *`)

**Executor** (`execute`) — evaluates WHERE expressions against record dicts and dispatches to `StorageEngine` calls.

**CLI modes:**
- Default: syntax check only
- `--debug-ast`: prints AST as JSON
- `--execute --db-dir ./data`: runs against the live storage engine

---

### Part 2 — Storage Engine (`storage.py`)

#### `StorageEngine`
Top-level entry point. Creates the data directory, loads the `Catalog`, and caches `TableHandle` instances. Provides DDL: `create_table`, `drop_table`, `open_table`, `list_tables`, `table_exists`.

#### `Catalog`
Persistent schema + Free Space Map (FSM) store serialised as `catalog.json`. Writes are **atomic**: data is written to `catalog.json.tmp` then renamed via `os.replace()` (POSIX atomic rename), so a mid-write crash cannot corrupt the file.

#### `TableHandle`
DML API for a single table. Provides:
- **`insert(record)`** — encodes to bytes, finds a page via FSM, writes, returns `RID`
- **`get(rid)`** — reads page, follows forwarding chains (max depth 16), decodes to dict
- **`update(rid, new_record)`** — tries in-place first; on overflow, inserts the new record elsewhere then installs a forwarding pointer at the original slot
- **`delete(rid)`** — follows forwarding chains, tombstones the actual data slot then the forwarding slot
- **`scan()`** — sequential page/slot traversal; skips deleted and MOVED_FLAG slots; always yields the original `RID`

#### `HeapFile`
File-level manager for a single `<table>.db` binary file. Reads/writes/allocates fixed 4096-byte pages. Every `write_page()` calls `flush()` + `os.fsync()` for crash durability.

#### `Page`
In-memory representation of a single 4 KB slotted page — the heart of the engine.

**Layout:**
```
[Header 16B][Record data →  ...free space...  ← Slot directory]
```
- **Header** (16 bytes): `page_id`, `num_slots`, `fss` (free-space start), `fse` (free-space end)
- **Slot directory** (grows backward from page end): each entry is 4 bytes `(offset: uint16, length: uint16)`
- **Record data region** (grows forward from byte 16)

**Three slot sentinel values:**

| Pattern | Meaning |
|---|---|
| `offset=0x0000, length=0x0000` | Deleted tombstone |
| `offset=0xFFFE, length=data_off` | Forwarding pointer — 8-byte target RID stored at `data_off` |
| `length & 0x8000` set | MOVED_FLAG — record is a forwarding target; scanner skips it |

Key methods: `write_record`, `read_record`, `update_inplace`, `delete_record`, `write_forward`, `compact`.

#### `RID` (NamedTuple)
Stable record identity: `(page_id, slot_id)`. Always resolves to the correct record — even after overflow updates that physically move the record to a new page/slot — because the engine follows forwarding chains transparently.

#### `ColumnDef` (dataclass)
Schema description for one column (`name`, `col_type`). Serialises to/from JSON for catalog persistence. Supported types: `INT`, `BIGINT`, `FLOAT`, `BOOL`, `TEXT`.

#### `encode_record` / `decode_record`
Converts between Python dicts and the on-disk binary format:
- **Null bitmap**: `⌈ncols/8⌉` bytes, one bit per column
- **Per-column data**: INT=4B, BIGINT=8B, FLOAT=8B (IEEE 754 big-endian), BOOL=1B, TEXT=2B length prefix + UTF-8
- **Minimum 8-byte padding** (`MIN_RECORD_SIZE`) ensures a forwarding pointer's 8-byte target RID can always be written in-place

#### Compression Utilities
- **`RLEEncoder`** — Run-Length Encoding for `INT`/`BIGINT` columns. Wire format: `[uint32 run_count][run × (value, uint32 count)]`. 1000 identical values compress to ~0.3% of original.
- **`DictEncoder`** — Dictionary encoding for `TEXT` columns. Wire format: `[uint16 dict_size][dict entries][uint32 n_values][uint16 codes]`. Low-cardinality text compresses to ~31% of original.
- **`scan_compressed(table, col_name)`** — extracts a full column via `scan()` and compresses it.

---

## Data Flow

### INSERT
```
User dict → encode_record() → bytes
         → Catalog.get_fsm() → find page with enough free space
         → HeapFile.read_page() or allocate_page()
         → Page.write_record() → advances fss, assigns slot_id
         → HeapFile.write_page() → fsync
         → Catalog.update_fsm() → atomic JSON write
         → return RID(page_id, slot_id)
```

### GET
```
RID(pid, sid)
→ HeapFile.read_page(pid)
→ Page.get_slot(sid) → (offset, length)
→ if tombstone → return None
→ if forward ptr → recurse with new RID (max depth 16)
→ read bytes → decode_record() → dict
```

### OVERFLOW UPDATE
```
new_data > old_slot_size:
→ insert(new_record, _moved=True)   [MOVED_FLAG on new slot]
→ re-read original page
→ Page.write_forward(old_slot, new_rid)   [8-byte RID at old data offset]
→ write_page() + update_fsm()
→ original RID now transparently resolves through the forward chain
```

### SCAN
```
for page in 0..page_count:
    for slot in 0..num_slots:
        deleted?        → skip
        is_moved_target? → skip (reachable only via forward ptr)
        is_forward?     → follow chain, yield (original_RID, record)
        else            → decode, yield (RID, record)
```

---

## Key Design Decisions

1. **Slotted heap pages (PostgreSQL-style)** — supports variable-length records; O(1) point access once a RID is known.

2. **Stable RIDs via forwarding pointers** — callers never need to update cached RIDs after overflow updates; the engine follows forward chains transparently on every `get`, `update`, and `delete`.

3. **MOVED_FLAG on relocated slots** — prevents `scan()` from double-counting records that have been overflow-updated.

4. **FSM in the catalog** — eliminates a full-file scan to find free space on insert. Trade-off: deleted record space is not returned to the FSM until `compact()` is called manually.

5. **Atomic catalog writes** — write-to-tmp-then-rename guarantees the schema/FSM is never half-written on disk.

6. **No buffer pool** — every page access opens and closes the file. Simple and correct; not high-throughput.

7. **No WAL** — crash-safe at single-page granularity only. Multi-page operations (overflow update) are not atomic across a crash. Acceptable for a course project.

8. **Big-endian wire format throughout** — consistent cross-platform portability of `.db` files.

9. **`MIN_RECORD_SIZE = 8` bytes padding** — every record is padded to at least 8 bytes, guaranteeing a forwarding pointer's 8-byte `(uint32 page_id, uint32 slot_id)` can always be written in-place without needing extra space.

---

## Test Coverage

**`test_storage.py`** (pytest, ~100 test methods across 8 classes):
- `TestPage` — header, slot CRUD, tombstones, forwarding pointers, MOVED_FLAG, `compact`
- `TestRecordEncoding` — all types, nulls, unicode, large TEXT, padding
- `TestHeapFile` — page I/O, file lifecycle, gap prevention
- `TestStorageCRUD` — full CRUD + scan, overflow update, multi-page
- `TestPersistence` — schema and data survive `StorageEngine` restart
- `TestEdgeCases` — page boundaries, slot reuse, forward chains, double-overflow
- `TestMultiTable` — table independence, drop-one-leave-other, separate files
- `TestCompression` — RLE + dict encode/decode/ratio, `scan_compressed`

**`test-db-cli.py`** (unittest): CREATE TABLE, INSERT, SELECT with complex WHERE, error cases.
