# Architecture: Flat-DB (Parts 2 & 3)

**Project:** Flat-DB  
**Track:** A — OLTP Row-Store (slotted heap pages)  
**Language:** Python 3.9+  
**Implementation:** `storage.py` · `engine.py` · `db_cli.py`

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Part 3 — Query Execution Engine](#2-part-3--query-execution-engine)
   - 2.1 [Pipeline Overview](#21-pipeline-overview)
   - 2.2 [AST Extensions (db_cli.py)](#22-ast-extensions-db_clipy)
   - 2.3 [Physical Operators](#23-physical-operators)
   - 2.4 [Expression Evaluator](#24-expression-evaluator)
   - 2.5 [Planner](#25-planner)
   - 2.6 [Output Formatter](#26-output-formatter)
   - 2.7 [Operator Composition Examples](#27-operator-composition-examples)
3. [Part 2 — Storage Layer](#3-part-2--storage-layer)
   - 3.1 [Component Breakdown](#31-component-breakdown)
   - 3.2 [On-Disk File Layout](#32-on-disk-file-layout)
   - 3.3 [Page Physical Layout](#33-page-physical-layout)
   - 3.4 [Slot Directory & Sentinel Values](#34-slot-directory--sentinel-values)
   - 3.5 [Record Encoding](#35-record-encoding)
   - 3.6 [RID Semantics & Stability](#36-rid-semantics--stability)
   - 3.7 [Free Space Management](#37-free-space-management)
   - 3.8 [Operation Walkthroughs](#38-operation-walkthroughs)
   - 3.9 [Forwarding Pointer Protocol](#39-forwarding-pointer-protocol)
   - 3.10 [Scan Correctness Invariant](#310-scan-correctness-invariant)
   - 3.11 [Persistence & Recovery Model](#311-persistence--recovery-model)
   - 3.12 [Compression Subsystem](#312-compression-subsystem)
4. [Complexity Analysis](#4-complexity-analysis)
5. [Known Limitations](#5-known-limitations)
6. [Module Map](#6-module-map)

---

## 1. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          db_cli.py                                   │
│                                                                      │
│  Lexer  ──►  Parser  ──►  AST                                        │
│                             │                                        │
│              ┌──────────────┴───────────────┐                        │
│              │  DDL (inline)                │  DML / SELECT          │
│              │  CREATE TABLE                ▼                        │
│              │  DROP TABLE          engine.build_plan()              │
│              └──────────────────────────────┘                        │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Operator tree
┌──────────────────────────────▼───────────────────────────────────────┐
│                          engine.py                                   │
│                                                                      │
│  Planner  ──►  Operator tree  ──►  run_plan()  ──►  stdout           │
│                                                                      │
│  SeqScan ─► Filter ─► [HashJoin] ─► [GroupByAggregate]              │
│                ─► [Sort] ─► [Limit] ─► Project                      │
│                                                                      │
│  InsertOp  /  DeleteOp  /  CTASop                                    │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Python API
┌──────────────────────────────▼───────────────────────────────────────┐
│                         storage.py                                   │
│                                                                      │
│  StorageEngine                                                       │
│    ├─ Catalog  (catalog.json — schema + FSM)                         │
│    └─ TableHandle  ×N                                                │
│         ├─ schema: List[ColumnDef]                                   │
│         └─ HeapFile  (<table>.db)                                    │
│              └─ Page  ×P  (4 096 bytes each)                         │
│                   ├─ Header       (16 B)                             │
│                   ├─ Record data  (grows →)                          │
│                   └─ Slot dir     (grows ←)                          │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ disk I/O
┌──────────────────────────────▼───────────────────────────────────────┐
│  data/                                                               │
│    catalog.json        ← schema + FSM (atomic JSON)                  │
│    employees.db        ← heap file (N × 4 096 B pages)               │
│    orders.db           ← heap file                                   │
│    …                                                                 │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Part 3 — Query Execution Engine

### 2.1 Pipeline Overview

The engine uses the **Volcano / iterator model**: every physical operator implements `open() / next() / close()`. Operators are composed into a tree; the root is pulled by the output formatter one row at a time.

```
SQL string
    │
    ▼  db_cli.Lexer + Parser
AST (SelectStmt / InsertStmt / DeleteStmt / CreateTableAsStmt)
    │
    ▼  engine.build_plan(stmt, engine)
Operator tree
    │
    ▼  engine.run_plan(plan, stmt, engine, out)
Tabular output  →  stdout
```

For a `SELECT` with all clauses the tree is:

```
Project  (SELECT list: column refs, scalar functions, aggregate labels)
  └─ Limit  (LIMIT n)
       └─ Sort  (ORDER BY col [ASC|DESC])
            └─ GroupByAggregate  (GROUP BY + SUM/AVG/MIN/MAX)
                 └─ Filter  (WHERE predicate)
                      └─ HashJoin  (equi-join on two SeqScans)
                           ├─ SeqScan  (left table)
                           └─ SeqScan  (right table)
```

Operators not required by the query are simply omitted from the tree.

---

### 2.2 AST Extensions (`db_cli.py`)

Part 3 adds the following nodes to the AST (all `dataclass`):

| Node | Used for |
|---|---|
| `FuncCall(func_name, args)` | Both scalar (`UPPER`, `LOWER`, `SPLIT`) and aggregate (`MAX`, `MIN`, `SUM`, `AVG`) calls |
| `OrderByKey(col, ascending)` | One key in an `ORDER BY` clause |
| `JoinClause(right_table, right_alias, condition, join_type)` | `[HASH\|INNER] JOIN … ON …` |
| `DeleteStmt(table_name, selection)` | `DELETE FROM t WHERE …` |
| `CreateTableAsStmt(table_name, select)` | `CREATE TABLE t AS SELECT …` |

`SelectStmt` gains four new optional fields: `from_alias`, `join`, `group_by`, `order_by`.

**New SQL grammar (additions over Part 2):**

```
SELECT  proj [, proj]
FROM    table [alias]
[ [HASH|INNER] JOIN table [alias] ON expr ]
[ WHERE expr ]
[ GROUP BY col [, col] ]
[ ORDER BY col [ASC|DESC] [, col [ASC|DESC]] ]
[ LIMIT n ]

DELETE FROM table [ WHERE expr ]

CREATE TABLE name AS SELECT …
```

`proj` can be `*`, a bare column name, a qualified `table.col`, or a function call such as `UPPER(col)` / `MAX(col)`.

**New keywords added to the Lexer:**  
`ORDER BY ASC DESC JOIN HASH INNER ON GROUP DELETE AS SUM AVG MIN MAX UPPER LOWER SPLIT`  
`.` added to `DELIM` for qualified column references (`t.col`).

---

### 2.3 Physical Operators

Each operator implements `open() / next() / close()` and is iterable.

#### `SeqScan`
Wraps `TableHandle.scan()`. Prefixes every column key with the table alias (`alias.col`) to allow `HashJoin` to distinguish columns from two tables.

#### `Filter`
Evaluates the predicate expression against each row via `eval_expr`. Rows for which the predicate returns falsy are discarded.

#### `Project`
Applied last in the SELECT pipeline. For each row:
- `'*'` projection: flattens qualified keys to bare names using suffix matching.
- Explicit list: calls `eval_expr(expr, row)` for each projection expression. Scalar function calls (`UPPER`, `LOWER`, `SPLIT`) are evaluated here; aggregate labels are looked up by key (pre-computed by `GroupByAggregate`).

Column header labels are derived by `_projection_label(expr)`:
- `ColumnRef("name")` → `"name"`, `ColumnRef("e.salary")` → `"salary"`
- `FuncCall("MAX", [ColumnRef("salary")])` → `"MAX(salary)"`

#### `Limit`
Counts pulled rows and returns `None` once the limit is reached, enabling early termination of the upstream pipeline.

#### `Sort`
Materialises all rows from the child operator, then sorts using Python's stable `sorted()`. Supports multiple keys applied in reverse priority order (so the first key takes precedence). `None` values sort last.

#### `HashJoin`
**Build phase** (`open`): materialises the entire right-side `SeqScan` into an in-memory hash table keyed on the right join column.  
**Probe phase** (`next`): pulls one row at a time from the left `SeqScan`, looks up the hash table, merges matching pairs. Only equi-joins are supported. Inner-join semantics: unmatched left rows are discarded.

Column resolution handles both qualified (`t.col`) and bare (`col`) names via suffix matching.

#### `GroupByAggregate`
Materialises all rows, then groups by the `GROUP BY` column list (each group key is a tuple). Applies one aggregate function (`SUM / AVG / MIN / MAX`) over the specified column within each group. `NULL` values are excluded from aggregation. The result rows contain group-by columns plus the aggregate result stored under the projection label (e.g. `"MAX(salary)"`).

Queries without `GROUP BY` but with an aggregate treat the entire result set as one group.

#### `InsertOp`
Coerces literal values to column types, calls `TableHandle.insert()`, yields a single summary row `{"result": "Inserted 1 row. RID=…"}`.

#### `DeleteOp`
Performs a full scan of the target table, collects matching RIDs (applying the WHERE predicate if present), then calls `TableHandle.delete()` for each. Yields a single summary row with the count deleted.

#### `CTASop`
Executes the inner SELECT plan fully, infers column types from the first result row, calls `engine.create_table()` to create the destination table, then inserts all rows.

---

### 2.4 Expression Evaluator

`eval_expr(expr, row: dict) → Any`

Handles all expression node types:

| Node type | Behaviour |
|---|---|
| `Literal` | Returns the stored Python value directly |
| `ColumnRef` | Looks up `row[name]`; falls back to suffix match `row[k]` where `k.endswith('.name')` for qualified references |
| `BinaryOp` | Short-circuits `AND`/`OR`; evaluates comparison operators `= != < > <= >=`; supports `+` (also string concat) and `-` |
| `FuncCall` (aggregate) | Looks up the pre-computed value by label in the row (the aggregate was computed by `GroupByAggregate`) |
| `FuncCall` (scalar) | Dispatches to `_apply_scalar` |

**Scalar functions:**

| Function | Input | Output |
|---|---|---|
| `UPPER(col)` | TEXT | Uppercase string |
| `LOWER(col)` | TEXT | Lowercase string |
| `SPLIT(col, sep)` | TEXT, TEXT | Parts joined with `\|` |

---

### 2.5 Planner

`build_plan(stmt, engine) → Operator`

Translates an AST statement into a physical operator tree. No logical plan layer — the mapping is direct AST → physical.

**SELECT planning algorithm:**

```
1. SeqScan(left_table, alias=from_alias or table_name)

2. if stmt.join:
       right_scan = SeqScan(right_table, alias=right_alias)
       extract equi-join keys from ON condition
       node = HashJoin(node, right_scan, left_key, right_key)

3. if stmt.selection:
       node = Filter(node, stmt.selection)

4. if stmt.group_by or any projection is an aggregate FuncCall:
       extract (agg_func, agg_col, agg_label) from projections
       node = GroupByAggregate(node, group_by, agg_func, agg_col, agg_label)

5. if stmt.order_by:
       node = Sort(node, stmt.order_by)

6. if stmt.limit:
       node = Limit(node, stmt.limit)

7. node = Project(node, stmt.projections, all_schema_cols)
```

**Other statements:**

| AST node | Operator |
|---|---|
| `InsertStmt` | `InsertOp(tbl, schema, values_exprs)` |
| `DeleteStmt` | `DeleteOp(tbl, predicate, alias)` |
| `CreateTableAsStmt` | `CTASop(new_table, build_plan(select), engine)` |
| `CreateTableStmt` / `_DropTableStmt` | Handled inline in `execute()` — no operator |

**`__main__` / module identity fix:**  
`main()` registers `sys.modules['db_cli'] = sys.modules[__name__]` before the first import of `engine`. This ensures that when `engine.py` does `import db_cli`, it receives the same module object (and therefore the same class objects) as the one that created the parsed AST. Without this, `isinstance` checks fail because running `python3 db_cli.py` loads the module as `__main__`, not `db_cli`.

---

### 2.6 Output Formatter

`run_plan(plan, stmt, engine, out)`

- **Mutation operators** (`InsertOp`, `DeleteOp`, `CTASop`): iterates the plan and prints the single result message.
- **Query operators**: opens the plan, drains all rows into a list, then prints:
  1. Header row: column labels separated by two spaces, padded to column width.
  2. Separator row: dashes.
  3. Data rows: values left-padded to column width.
  4. Footer: `(N rows)`.

Column width is the maximum of 14 and the longest label + 2.

---

### 2.7 Operator Composition Examples

**`SELECT name, salary FROM emp WHERE salary > 80000 ORDER BY salary DESC LIMIT 3`**

```
Project(["name","salary"])
  └─ Limit(3)
       └─ Sort([OrderByKey("salary", ascending=False)])
            └─ Filter(BinaryOp(ColumnRef("salary"), ">", Literal(80000)))
                 └─ SeqScan(emp)
```

**`SELECT dept_id, MAX(salary) FROM emp GROUP BY dept_id`**

```
Project(["dept_id", "MAX(salary)"])
  └─ GroupByAggregate(group_by=["dept_id"], func=MAX, col="salary", label="MAX(salary)")
       └─ SeqScan(emp)
```

**`SELECT e.name, d.title FROM emp e JOIN dept d ON e.dept_id = d.id`**

```
Project(["name","title"])
  └─ HashJoin(left_key="e.dept_id", right_key="d.id")
       ├─ SeqScan(emp, alias="e")
       └─ SeqScan(dept, alias="d")
```

**`SELECT UPPER(name), salary FROM emp WHERE salary > 70000`**

```
Project([FuncCall("UPPER", [ColumnRef("name")]), ColumnRef("salary")])
  └─ Filter(salary > 70000)
       └─ SeqScan(emp)
```

---

## 3. Part 2 — Storage Layer

### 3.1 Component Breakdown

| Class / Function | Role |
|---|---|
| `StorageEngine` | Entry point. Creates/drops/opens tables; owns the `Catalog`. |
| `Catalog` | Loads and atomically saves `catalog.json`. Stores schema and the per-page free-space map (FSM) for every table. |
| `TableHandle` | DML API for one table: `insert / get / update / delete / scan`. Owns a `HeapFile` and a `List[ColumnDef]`. |
| `HeapFile` | File-level manager. Reads/writes/allocates fixed-size 4 KB pages; calls `fsync` on every write. |
| `Page` | In-memory 4 KB `bytearray`. Encodes/decodes the header, slot directory, and record bytes. Stateless between calls; callers write it back via `HeapFile`. |
| `encode_record` | Converts a `dict` to a compact byte string using the null-bitmap + per-type encoding. |
| `decode_record` | Inverse of `encode_record`. |
| `RLEEncoder` | Run-Length Encoding for `INT` / `BIGINT` column batches. |
| `DictEncoder` | Dictionary encoding for `TEXT` column batches. |
| `scan_compressed` | Extracts one column from a `TableHandle` and returns it compressed. |
| `RID` | `NamedTuple(page_id, slot_id)` — the stable record identity. |
| `ColumnDef` | `dataclass(name, col_type)` — one column in a schema. |

---

### 3.2 On-Disk File Layout

```
<data_dir>/
  catalog.json          ← single JSON file, all tables
  catalog.json.tmp      ← transient during atomic write (rename on close)
  <table_name>.db       ← heap file for table "table_name"
  …
```

**`catalog.json` structure:**

```json
{
  "tables": {
    "employees": {
      "schema": [
        {"name": "id",     "type": "INT"},
        {"name": "name",   "type": "TEXT"},
        {"name": "salary", "type": "FLOAT"},
        {"name": "active", "type": "BOOL"}
      ],
      "page_count": 3,
      "fsm": [120, 4040, 3900]
    }
  }
}
```

`fsm[i]` = bytes of contiguous free space on page `i` = `page.fse − page.fss`.

---

### 3.3 Page Physical Layout

```
Offset  0                                            4095
        ┌────────────┬──────────────────────┬───────────────┐
        │  Header    │   Record data  →     │  ← Slot dir   │
        │  (16 B)    │                      │               │
        └────────────┴──────────────────────┴───────────────┘
                     ↑                      ↑
                    fss                    fse
                (free_space_start)    (free_space_end)

Free bytes = fse − fss
```

**Header (16 bytes, big-endian):**

```
Byte  0- 3   page_id    uint32
Byte  4- 5   num_slots  uint16
Byte  6- 7   fss        uint16   next write offset (grows →)
Byte  8- 9   fse        uint16   first byte of slot directory (grows ←)
Byte 10-11   flags      uint16   reserved
Byte 12-15   reserved   uint32
```

struct format: `'>IHHHHI'`

---

### 3.4 Slot Directory & Sentinel Values

Grows from the **end** of the page toward lower addresses. Slot `i` lives at page offset `4096 − (i+1) × 4`. Each slot entry is 4 bytes (big-endian): `(offset: uint16, length: uint16)`.

| `offset` | `length` | Meaning |
|---|---|---|
| `0x0000` | `0x0000` | **Deleted tombstone** |
| `0xFFFE` | `data_off` | **Forwarding pointer** — 8 bytes at page offset `data_off & 0x7FFF` hold `(target_page_id: uint32, target_slot_id: uint32)`. If `data_off & 0x8000`, this slot was a moved-target before re-forwarding (scanner skips it). |
| `≥ 16` | `len \| 0x8000` | **Moved-target record** — live but reachable only through a forwarding pointer; scanner skips it. Real length = `length & 0x7FFF`. |
| `≥ 16` | `len` (bit 15 clear) | **Live record** — normal case. |

---

### 3.5 Record Encoding

```
[ null_bitmap: ⌈ncols / 8⌉ bytes ]   bit i = 1 → column i is NULL
[ per-column bytes, schema order, skipping NULLs ]
[ zero-padding to MIN_RECORD_SIZE = 8 bytes ]
```

All multi-byte values are **big-endian**.

| Type | Encoding | Size |
|---|---|---|
| `INT` | signed 32-bit | 4 B |
| `BIGINT` | signed 64-bit | 8 B |
| `FLOAT` | IEEE 754 double | 8 B |
| `BOOL` | unsigned byte (0 or 1) | 1 B |
| `TEXT` | uint16 length prefix + UTF-8 | 2 + n B |

The 8-byte minimum padding guarantees that a forwarding pointer's `(uint32, uint32)` target RID can always be written in-place at the record's original offset.

---

### 3.6 RID Semantics & Stability

`RID = (page_id: int, slot_id: int)` — issued once by `insert()` and never reissued.

- Stable through **in-place updates** (new data fits in the old slot).
- Stable through **overflow updates** (engine installs a forwarding pointer; all subsequent `get / update / delete` follow the chain transparently, depth ≤ 16).
- Returns `None` after `delete`.

---

### 3.7 Free Space Management

`free_bytes(page) = page.fse − page.fss`

The `Catalog` stores `fsm[page_id] = free_bytes` for every page of every table. Updated atomically after each page mutation. On insert, the FSM is scanned linearly for the first page with `free_bytes ≥ record_size + SLOT_SIZE`; if none fits, a new page is appended.

**Trade-off:** tombstoned record bodies are not reflected in the FSM. Deleted space is only reclaimed by calling `Page.compact()` explicitly.

---

### 3.8 Operation Walkthroughs

**insert:**
```
encode_record() → bytes
  → FSM lookup → find/allocate page
  → Page.write_record()
  → HeapFile.write_page()  [fsync]
  → Catalog.update_fsm()   [atomic rename]
  → return RID
```

**get:**
```
RID → read_page → get_slot → (offset, length)
  tombstone  → None
  forward    → read_forward_target() → recurse
  live       → buf[offset:offset+len] → decode_record() → dict
```

**overflow update:**
```
new_len > old_slot_size
  → insert(new_record, _moved=True)          # MOVED_FLAG on new slot
  → re-read original page
  → Page.write_forward(old_slot, new_rid)    # 8-byte RID at old offset
  → write_page() + update_fsm()
```

**delete:**
```
RID → get_slot
  is_forward? → delete target chain first, then tombstone this slot
  else        → zero record bytes → slot = (0x0000, 0x0000)
```

**scan:**
```
for page in 0..page_count-1:
  for slot in 0..num_slots-1:
    deleted?         → skip
    is_moved_target? → skip
    is_forward?      → follow chain, yield (original RID, record)
    live             → decode, yield (RID, record)
```

---

### 3.9 Forwarding Pointer Protocol

**Slot state machine:**

```
EMPTY ──insert()──► LIVE
                      │
          in-place     │  overflow update
          update       ▼
             │    FORWARD_PTR ──────► (chain target)
             │         │
             │    delete()           delete()
             ▼         ▼                 │
           LIVE    TOMBSTONE        TOMBSTONE
             │
          delete()
             ▼
         TOMBSTONE
```

When `write_forward(slot_id, target_page, target_slot)` is called:
1. Overwrites the old record's first 8 bytes with `struct.pack('>II', target_page, target_slot)`.
2. Sets slot to `(FORWARD_MARKER=0xFFFE, old_off | (MOVED_FLAG if was_moved_target else 0))`.

MOVED_FLAG propagation into re-chained forward-pointer slots is what preserves correct scan behaviour through multiple overflow updates on the same record.

---

### 3.10 Scan Correctness Invariant

Every live record is yielded **exactly once** by `scan()`, always with its **original** RID.

1. Direct live slots (no MOVED_FLAG) → yielded directly.
2. Forwarding-pointer slots (no MOVED_FLAG in length) → followed, yielded with original RID.
3. MOVED_FLAG live slots → skipped (reachable only through their originating forward pointer).
4. MOVED_FLAG forwarding-pointer slots → skipped by `is_moved_target()`.
5. Tombstones → skipped.

`update_inplace()` preserves MOVED_FLAG on moved-target slots, preventing them from being directly exposed even after their content is updated in place.

---

### 3.11 Persistence & Recovery Model

**Page durability:**
```
HeapFile.write_page(page):
    f.write(page.as_bytes())    # full 4096 bytes
    f.flush()
    os.fsync(f.fileno())        # kernel buffer → disk
```

**Catalog atomicity:**
```
Catalog._save():
    write  catalog.json.tmp  (full JSON)
    os.replace(.tmp, catalog.json)   # POSIX atomic rename
```

A crash leaves either the old or new catalog fully intact. No WAL; crash-safe at single-page granularity only.

**What is NOT provided:** WAL, undo log, page-level checksums, crash recovery scan, buffer pool, multi-process concurrency.

---

### 3.12 Compression Subsystem

Operates on column batches extracted by a full `scan()`. Separate from the row-store; used for analytics.

**RLE (`RLEEncoder`)** — for `INT`/`BIGINT` columns with repeated values:
```
[uint32 run_count]  [run_count × (value 4B|8B, count uint32)]
```
1 000 identical values: 12 B vs 4 000 B → **0.3% of original**.

**Dictionary encoding (`DictEncoder`)** — for low-cardinality `TEXT` columns:
```
[uint16 dict_size]  [dict entries]  [uint32 n_values]  [uint16 codes]
```
130 rows, 3 distinct strings of ~5 chars: ~286 B vs ~910 B → **31% of original**.

`scan_compressed(table, col_name)` extracts one column and compresses it (`INT`/`BIGINT` → RLE, `TEXT` → Dict).

---

## 4. Complexity Analysis

| Operation | Time | Notes |
|---|---|---|
| `insert` | **O(1)** amortised | FSM linear scan O(P) worst case |
| `get` | **O(1)** | One page seek; O(1) per forward hop (depth ≤ 16) |
| `update` in-place | **O(1)** | One page read + write |
| `update` overflow | **O(1)** + insert | Two page writes |
| `delete` | **O(1)** | One page read + write per hop |
| `scan` | **O(N)** | N = live records, sequential pages |
| `Filter` | **O(N)** | Per-row predicate evaluation |
| `Sort` | **O(N log N)** | Materialises full result set |
| `HashJoin` | **O(L + R)** | Build hash table on R rows, probe with L rows |
| `GroupByAggregate` | **O(N)** | Materialises, then one linear pass |
| `Limit` | **O(K)** | Early stop after K rows (upstream truncated) |
| `scan_compressed` | **O(N)** | Full scan + linear encode |

---

## 5. Known Limitations

| Limitation | Impact |
|---|---|
| No WAL / transaction log | Multi-page operations (overflow update) are not atomic at crash time |
| No page compaction on delete | Deleted bytes are stranded until `Page.compact()` is called manually |
| FSM does not track tombstone space | A page with many deleted records may still appear full to the FSM |
| No buffer pool | Every page access opens/closes the file; no caching |
| Forward chain depth ≤ 16 | More than 16 chained overflow updates on one record raises `RuntimeError` |
| Hash join fully materialises right side | Large right tables exhaust memory |
| Sort fully materialises | `ORDER BY` on very large result sets exhaust memory |
| No indexes | All lookups except direct `get(rid)` require a full scan |
| No multi-process concurrency | No file locking; simultaneous writers corrupt state |
| Single aggregate per query | Only one `SUM/AVG/MIN/MAX` per `SELECT` list is supported |
| Dictionary encoder limited to uint16 codes | Max 65 535 distinct TEXT values per column batch |

---

## 6. Module Map

```
db_cli.py
│
├── AST nodes
│     Expr, ColumnRef, Literal, BinaryOp, FuncCall
│     OrderByKey, JoinClause
│     Statement, CreateTableStmt, CreateTableAsStmt
│     InsertStmt, SelectStmt, DeleteStmt, _DropTableStmt
│
├── Lexer  (regex tokeniser)
│
├── Parser  (recursive descent)
│     parse_create / parse_drop / parse_insert / parse_select
│     parse_delete / parse_scan
│     parse_expression → parse_or → parse_and → parse_comparison
│                      → parse_additive → parse_primary
│
├── execute(stmt, engine, out)
│     DDL inline  →  engine.create_table / drop_table
│     DML/SELECT  →  engine.build_plan() + run_plan()
│
└── main()
      sys.modules['db_cli'] = sys.modules[__name__]  ← __main__ fix
      argparse: --query --execute --db-dir --debug-ast

engine.py
│
├── eval_expr(expr, row)          ← expression evaluator
├── _apply_scalar(fn, args, row)  ← UPPER / LOWER / SPLIT
├── _projection_label(expr)       ← column header names
│
├── Operator  (ABC: open / next / close / __iter__)
├── SeqScan      — TableHandle.scan() wrapper + alias prefix
├── Filter       — predicate push-down
├── Project      — SELECT list + scalar function evaluation
├── Limit        — early-stop
├── Sort         — materialise + sorted()
├── HashJoin     — build hash on right, probe with left
├── GroupByAggregate  — materialise + group + SUM/AVG/MIN/MAX
├── InsertOp     — TableHandle.insert()
├── DeleteOp     — scan + filter + TableHandle.delete()
├── CTASop       — run SELECT plan + create_table + bulk insert
│
├── build_plan(stmt, engine) → Operator   ← planner
└── run_plan(plan, stmt, engine, out)     ← output formatter

storage.py
│
├── Constants  (PAGE_SIZE, HEADER_SIZE, SLOT_SIZE, MIN_RECORD_SIZE,
│               FORWARD_MARKER, DELETED_OFFSET, MOVED_FLAG)
├── RID  (NamedTuple: page_id, slot_id)
├── ColumnDef  (dataclass: name, col_type)
├── Page       (in-memory 4 KB buffer)
├── HeapFile   (file of fixed-size pages, fsync on write)
├── encode_record / decode_record
├── Catalog    (catalog.json: schema + FSM, atomic writes)
├── TableHandle  (insert / get / update / delete / scan)
├── StorageEngine  (DDL: create/drop/open/list tables)
└── Compression
      RLEEncoder.encode / decode / ratio
      DictEncoder.encode / decode / ratio
      scan_compressed(table, col_name) → bytes
```
