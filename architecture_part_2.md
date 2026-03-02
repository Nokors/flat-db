# Architecture: Part 2 — Storage Layer

**Project:** Flat-DB  
**Track:** A — OLTP Row-Store (slotted heap pages)  
**Language:** Python 3.9+  
**Implementation:** `storage.py` (1 046 lines)

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Component Breakdown](#2-component-breakdown)
3. [On-Disk File Layout](#3-on-disk-file-layout)
4. [Page Physical Layout](#4-page-physical-layout)
5. [Slot Directory & Sentinel Values](#5-slot-directory--sentinel-values)
6. [Record Encoding](#6-record-encoding)
7. [RID Semantics & Stability](#7-rid-semantics--stability)
8. [Free Space Management](#8-free-space-management)
9. [Operation Walkthroughs](#9-operation-walkthroughs)
10. [Forwarding Pointer Protocol](#10-forwarding-pointer-protocol)
11. [Scan Correctness Invariant](#11-scan-correctness-invariant)
12. [Persistence & Recovery Model](#12-persistence--recovery-model)
13. [Compression Subsystem](#13-compression-subsystem)
14. [Complexity Analysis](#14-complexity-analysis)
15. [Known Limitations](#15-known-limitations)
16. [Module Map](#16-module-map)

---

## 1. High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                         db_cli.py                             │
│   SQL Parser  ──►  Executor  ──►  StorageEngine API call      │
└──────────────────────────┬────────────────────────────────────┘
                           │  Python API
┌──────────────────────────▼────────────────────────────────────┐
│                       storage.py                              │
│                                                               │
│  StorageEngine                                                │
│    ├─ Catalog  (catalog.json — schema + FSM)                  │
│    └─ TableHandle  ×N                                         │
│         ├─ schema: List[ColumnDef]                            │
│         └─ HeapFile  (<table>.db)                             │
│              └─ Page  ×P  (4 096 bytes each)                  │
│                   ├─ Header       (16 B)                      │
│                   ├─ Record data  (grows →)                   │
│                   └─ Slot dir     (grows ←)                   │
└───────────────────────────────────────────────────────────────┘
                           │  disk I/O
┌──────────────────────────▼────────────────────────────────────┐
│  data/                                                        │
│    catalog.json        ← schema + FSM (atomic JSON)          │
│    employees.db        ← heap file (N × 4 096 B pages)       │
│    orders.db           ← heap file                           │
│    …                                                          │
└───────────────────────────────────────────────────────────────┘
```

**Call chain for a typical `insert`:**

```
user code
  → TableHandle.insert(record)
      → encode_record(schema, record)          # in-memory bytes
      → Catalog.get_fsm()                      # find target page
      → HeapFile.read_page(pid)  /  .allocate_page()
      → Page.write_record(data)                # mutate buffer
      → HeapFile.write_page(page)              # fsync to disk
      → Catalog.update_fsm(pid, free_bytes)    # atomic catalog write
      → return RID(page_id, slot_id)
```

---

## 2. Component Breakdown

| Class / Function | Role |
|-----------------|------|
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

## 3. On-Disk File Layout

```
<data_dir>/
  catalog.json          ← single JSON file, all tables
  catalog.json.tmp      ← transient during atomic write (rename on close)
  <table_name>.db       ← heap file for table "table_name"
  <table_name>.db       ← …one file per table
```

### `catalog.json` schema

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

`fsm[i]` = bytes of contiguous free space remaining on page `i` = `page.fse − page.fss`.  
Used by `insert` to locate a page with enough room without scanning the heap.

### Heap file (`.db`)

A flat binary file whose size is always an exact multiple of `PAGE_SIZE = 4096`.  
Page `i` occupies bytes `[i × 4096, (i+1) × 4096)`.  
Pages are appended; no internal fragmentation at the file level.

---

## 4. Page Physical Layout

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

### Header (16 bytes, big-endian)

```
Byte  0- 3   page_id    uint32   Persistent 0-based page number
Byte  4- 5   num_slots  uint16   Total slot entries allocated
Byte  6- 7   fss        uint16   Next byte to write a record (grows →)
Byte  8- 9   fse        uint16   First byte of slot directory (grows ←)
Byte 10-11   flags      uint16   Reserved
Byte 12-15   reserved   uint32   Reserved
```

struct format string: `'>IHHHHI'`  
`struct.calcsize('>IHHHHI') == 16` ✓

### Slot directory

Grows from the **end** of the page toward lower addresses.  
Slot `i` lives at page offset `4096 − (i+1) × 4`.

```
                slot 0   slot 1   slot 2
               ┌──────┬──────┬──────┐
 … free space … │ o0│l0│ o1│l1│ o2│l2│
               └──────┴──────┴──────┘
               ↑ 4088  4084  4080
              fse (initially 4096)
```

Each slot entry is 4 bytes, big-endian:

```
offset  uint16   Byte position of the record within this page
length  uint16   Byte length of the record, with flag bit 15
```

---

## 5. Slot Directory & Sentinel Values

| `offset` | `length` | Meaning |
|---------|---------|---------|
| `0x0000` | `0x0000` | **Deleted tombstone.** Record is logically gone. |
| `0xFFFE` | `data_off` | **Forwarding pointer.** 8 bytes at page offset `data_off & 0x7FFF` hold `(target_page_id: uint32, target_slot_id: uint32)`. If `data_off & 0x8000` (MOVED_FLAG), this slot was previously a moved-target before being re-forwarded (scanner skips it). |
| any `≥ 16` | `len \| 0x8000` | **Moved-target record.** Record is live and readable but was relocated here by an overflow update. Scanner skips it; only reachable through a forwarding pointer. Real byte length = `length & 0x7FFF`. |
| any `≥ 16` | `len` (bit 15 clear) | **Live record.** Normal case. |

### Why offsets `≥ 16`?

Valid record offsets start at `HEADER_SIZE = 16`. Using `0x0000` and `0xFFFE` as sentinels is therefore unambiguous with any real data offset.

### MOVED_FLAG bit-packing rationale

Max record size on a 4 KB page is `4096 − 16 = 4080 < 0x8000 = 32768`. So bit 15 of the length field can never be set by a legitimate record length, making it safe as a flag.

---

## 6. Record Encoding

### Wire format

```
[ null_bitmap: ⌈ncols / 8⌉ bytes ]
  bit i = 1  →  column i is NULL (no bytes follow for this column)
  bit i = 0  →  column i is present

[ column 0 bytes ]
[ column 1 bytes ]
  …
[ padding to MIN_RECORD_SIZE = 8 bytes ]
```

All multi-byte values are **big-endian**.

### Per-type encoding

| Column type | Encoded as | Size |
|-------------|-----------|------|
| `INT`    | signed 32-bit integer | 4 B |
| `BIGINT` | signed 64-bit integer | 8 B |
| `FLOAT`  | IEEE 754 double       | 8 B |
| `BOOL`   | unsigned byte (0 or 1)| 1 B |
| `TEXT`   | uint16 byte-length prefix + UTF-8 bytes | 2 + n B |

### Minimum record size padding

Every encoded record is zero-padded to **8 bytes** (`MIN_RECORD_SIZE`). This guarantees that the forwarding-pointer mechanism can always overwrite the first 8 bytes of any record with a `(page_id: uint32, slot_id: uint32)` target RID.

### Example — `{"id": 42, "name": "Hi", "active": None}`

Schema: `[INT id, TEXT name, BOOL active]`

```
null_bitmap  = 0b00000100  (bit 2 = active is NULL)  → 1 byte
id           = struct.pack('>i', 42)                  → 4 bytes  (0x0000002A)
name         = struct.pack('>H', 2) + b'Hi'           → 4 bytes
active       = (skipped, NULL)
padding      = 0 bytes                                (total = 9 ≥ 8)

Raw bytes: 04  00 00 00 2A  00 02 48 69
           ^   ^---------^  ^---^---^
           bitmap  id       len  H  i
```

---

## 7. RID Semantics & Stability

```
RID = (page_id: int, slot_id: int)
```

- `page_id` is the 0-based index of the page within `<table>.db`.
- `slot_id` is the 0-based index within that page's slot directory.
- A RID is **issued once** by `insert()` and **never reissued** for a different record.
- A RID remains valid through **in-place updates** (record grows to fit old slot).
- A RID remains valid through **overflow updates** (engine writes a forwarding pointer; the original RID still resolves via `get / update / delete`).
- A RID becomes a null resolve (`get → None`) after `delete`.
- **Slot index reuse:** when a slot is tombstoned, its *index* may be reused by the next `insert` that targets the same page. The reused slot gets a fresh record — the old (page_id, slot_id) now correctly returns `None` (tombstone sentinel), and the new record gets the same numeric (page_id, slot_id) pair. This is acceptable because callers should not hold RIDs to deleted records.

### Free-list behaviour

`Page._find_reusable_slot()` scans slots 0…num_slots−1 for the first tombstone `(0x0000, 0x0000)` and returns its index. If none exists, `num_slots` is returned (new entry, `fse` shrinks by 4). This reuse saves slot-directory space but does not reclaim record-data bytes (only `compact()` does that).

---

## 8. Free Space Management

### Per-page accounting

```
free_bytes(page) = page.fse − page.fss
```

`fss` advances on every `write_record`. `fse` retreats by 4 on every new slot entry (reusing a tombstone slot does not move `fse`). Neither pointer retreats on delete or forward-pointer creation, so free space is **monotonically consumed** until `compact()` or page replacement.

### Free Space Map (FSM)

The `Catalog` stores a per-table list `fsm[page_id] = free_bytes`. Updated atomically after every page mutation. On `insert`, the FSM is consulted linearly to find the first page with `free_bytes ≥ record_size + SLOT_SIZE`. If none exists, a new page is allocated.

```
Insert free-space lookup:
  for pid, free in enumerate(fsm):
      if free >= needed: use page pid
  else: allocate new page
```

**Trade-off:** the FSM does not account for space in tombstoned record bodies (those bytes are logically free but not tracked). The FSM is conservative: it only tracks the contiguous gap between `fss` and `fse`. Deleted record space is reclaimed only by calling `Page.compact()`.

---

## 9. Operation Walkthroughs

### insert

```
record  →  encode_record()  →  data (bytes)
                                   │
                           FSM lookup
                                   │
                    ┌──────────────┴──────────────┐
               page found                  no page fits
                    │                             │
            read_page(pid)             allocate_page() (append)
                    │                             │
                    └──────────────┬──────────────┘
                             Page.write_record(data)
                               │  append at fss
                               │  assign slot
                               │  advance fss, retreat fse
                            write_page()  [fsync]
                            update_fsm()  [atomic JSON]
                               │
                            return RID(pid, slot_id)
```

### get

```
RID(pid, sid)
    │
read_page(pid)
    │
get_slot(sid)  →  (offset, length)
    │
    ├─ offset == 0x0000  →  None   (deleted)
    ├─ offset == 0xFFFE  →  read_forward_target()  →  recurse
    └─ otherwise         →  buf[offset : offset + (length & 0x7FFF)]
                             │
                         decode_record()  →  dict
```

### update  (in-place path)

```
RID  →  read_page  →  encode new_record  →  Page.update_inplace()
          │
          └─ new len ≤ old len?  YES  →  overwrite bytes in buf
                                         preserve MOVED_FLAG bit
                                         write_page() + update_fsm()
                                         return True
```

### update  (overflow path)

```
new len > old len
    │
    ├─ insert(new_record, _moved=True)
    │      writes to a page with enough space
    │      sets MOVED_FLAG on the new slot
    │      returns new_rid
    │
    ├─ re-read original page  (insert may have modified same page)
    │
    ├─ Page.write_forward(old_slot, new_rid.page_id, new_rid.slot_id)
    │      stores 8-byte RID at old record's byte location
    │      sets slot  →  (0xFFFE,  data_off | MOVED_FLAG_if_was_moved)
    │
    └─ write_page() + update_fsm()
```

### delete  (direct slot)

```
RID  →  read_page  →  get_slot
    │
    ├─ is_forward?  YES  →  _delete_chain(target)  →  tombstone target
    │                       then tombstone this forward-ptr slot
    │
    └─ NO  →  zero record bytes  →  set slot = (0x0000, 0x0000)
              write_page() + update_fsm()
```

### scan

```
for page_id in 0 .. page_count-1:
    read_page(page_id)
    for slot_id in 0 .. num_slots-1:
        is_deleted?        → skip
        is_moved_target?   → skip   (reachable only via forward chain)
        is_forward?        → _get_chain(RID(page_id, slot_id))
                              yield  (RID(page_id, slot_id), record)
        else               → decode record bytes
                              yield  (RID(page_id, slot_id), record)
```

---

## 10. Forwarding Pointer Protocol

### State machine for a slot

```
          insert()
EMPTY  ──────────────►  LIVE
                          │
              in-place     │  overflow
              update       │  update
                 │         ▼
                 │  LIVE ──────────►  FORWARD_PTR  ──────►  (chain)
                 │                        │
                 │       delete()         │  delete()
                 ▼           │            ▼
              LIVE  ◄────────┘       TOMBSTONE
                 │
              delete()
                 │
                 ▼
            TOMBSTONE
```

### Slot encoding for forwarding

When `write_forward(slot_id, target_page, target_slot)` is called:

1. The old record's 8 bytes at offset `old_off` are overwritten with `struct.pack('>II', target_page, target_slot)`.
2. The slot becomes `(FORWARD_MARKER=0xFFFE, old_off | (MOVED_FLAG if was_moved_target else 0))`.

The `MOVED_FLAG` propagation into the "length" field of the forward-pointer slot is the key to correct scan behaviour when overflow updates chain.

### Double-overflow example

```
Initial:  page 0, slot 0  →  LIVE  ('v0')

Overflow update 1  →  insert 'X'*2000 as MOVED_TARGET  →  RID(0,1)
                      slot 0  →  FORWARD(0xFFFE, off_A)        ← no MOVED_FLAG
                      slot 1  →  LIVE | MOVED_FLAG              ← 'X'*2000

Overflow update 2  →  cannot fit in slot 1 (old=~2000, new=3000)
                      insert 'Y'*3000 as MOVED_TARGET  →  RID(1,0)
                      write_forward(slot_id=1, 1, 0):
                          was_moved = True   ← slot 1 had MOVED_FLAG
                          slot 1  →  FORWARD(0xFFFE, off_B | 0x8000)  ← MOVED_FLAG preserved

Scan result:
  slot 0:  is_moved_target? NO (no MOVED_FLAG in length)
           is_forward?      YES  →  follow: 0→1→(1,0)  →  'Y'*3000
           yield (RID(0,0), 'Y'*3000)   ← single result, correct RID

  slot 1:  is_moved_target? YES (MOVED_FLAG in length of forward-ptr slot)  →  skip

  page 1, slot 0:  is_moved_target? YES (MOVED_FLAG on live slot)  →  skip
```

---

## 11. Scan Correctness Invariant

**Invariant:** every live record is yielded *exactly once* by `scan()`, always with its *original* RID.

This holds because:

1. Direct live slots (no MOVED_FLAG) → yielded directly.
2. Forwarding-pointer slots (no MOVED_FLAG in length) → followed, yielded with original RID.
3. MOVED_FLAG live slots → skipped (reachable only through their originating forward pointer).
4. MOVED_FLAG forwarding-pointer slots (re-chained moved targets) → skipped by `is_moved_target()`.
5. Tombstones → skipped.

The MOVED_FLAG is **preserved** through `update_inplace()` (when in-place update is applied to a moved-target slot, bit 15 stays set). This prevents the slot from being directly exposed by future scans even after the record content is updated in place.

---

## 12. Persistence & Recovery Model

### Write durability

```
HeapFile.write_page(page):
    f.seek(page_id * PAGE_SIZE)
    f.write(page.as_bytes())    # full 4096 bytes
    f.flush()
    os.fsync(f.fileno())        # kernel → disk
```

Every page mutation is durable before the call returns.

### Catalog atomicity

```
Catalog._save():
    write  catalog.json.tmp  (full JSON)
    os.replace(.tmp, catalog.json)   # POSIX atomic rename
```

A crash during the catalog write either leaves the old `catalog.json` intact (if the rename did not complete) or the new one visible (if it did). The `.tmp` file is an orphan in the former case and is re-created on the next write.

### Restart behaviour

```
StorageEngine.__init__(data_dir):
    load catalog.json              → schema + FSM for all tables
    TableHandle.__init__:
        HeapFile(path)             → count existing pages from file size
        schema = catalog.get_schema()
        # ready to serve — no recovery scan needed
```

No WAL or redo log exists. The engine is crash-safe at **single-page granularity**: a crash that interrupts a multi-page operation (e.g. an overflow update that spans two pages) can leave those two pages in a partially-updated state. In practice, `write_page` is called separately for each page (old page forwarded, then new page written), so partial corruption is bounded to one record per operation.

### What is NOT provided

- No WAL / redo log.
- No undo log / transaction rollback.
- No page-level checksums.
- No crash recovery scan.

---

## 13. Compression Subsystem

The compression utilities operate on **column batches** extracted by a full `scan()`. They are separate from the row-store storage and are used for analytics.

### RLE (Run-Length Encoding) — `RLEEncoder`

Designed for `INT` / `BIGINT` columns with repeated values (e.g. status codes, category IDs).

**Wire format:**

```
[uint32 run_count]
[run_count × ( value: 4B or 8B signed  |  count: uint32 )]
```

**Example:** `[1, 1, 1, 2, 2, 3]` → 1 run × 3 + 1 run × 2 + 1 run × 1 = 3 runs

```
Encoded: 00 00 00 03          ← 3 runs
         00 00 00 01  00 00 00 03   ← value=1, count=3
         00 00 00 02  00 00 00 02   ← value=2, count=2
         00 00 00 03  00 00 00 01   ← value=3, count=1
Total: 4 + 3×(4+4) = 28 bytes   vs  6×4 = 24 bytes (worse for this tiny example)
```

For 1 000 identical values: `4 + 1×(4+4) = 12 B` vs `4000 B` → **0.3% of original**.

### Dictionary Encoding — `DictEncoder`

Designed for `TEXT` columns with low cardinality (e.g. status strings, country names).

**Wire format:**

```
[uint16 dict_size]
[dict_size × ( uint16 str_byte_len | UTF-8 bytes )]
[uint32 value_count]
[value_count × uint16 code]
```

**Example:** `['North', 'South', 'North', 'East', 'North']`

```
dict:   { 'North'→0, 'South'→1, 'East'→2 }
codes:  [0, 1, 0, 2, 0]
Header: 2 (dict_size)
Dict:   (5, b'North') (5, b'South') (4, b'East')
Values: 5 × uint16 = 10 bytes
```

For 130 rows with 3 distinct strings of ~5 chars each: `~286 B` vs `~910 B` → **31% of original**.

### `scan_compressed(table, col_name) → bytes`

Convenience function: runs a full `scan()` on the live table, extracts one column, and compresses it.

```
INT / BIGINT  →  RLEEncoder.encode(values, col_type)
TEXT          →  DictEncoder.encode(values)
other types   →  TypeError
```

---

## 14. Complexity Analysis

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| `insert` | **O(1)** amortised | O(record) | FSM linear scan in worst case O(P); new page allocation O(1). |
| `get` | **O(1)** | O(1) | One page seek + slot decode; +O(1) per forwarding hop (depth ≤ 16). |
| `update` (in-place) | **O(1)** | O(record) | One page read + write. |
| `update` (overflow) | **O(1)** + insert | O(record) | Two page writes (original + new). |
| `delete` | **O(1)** | O(1) | One page read + write; +one extra page per forwarding hop. |
| `scan` | **O(N)** | O(1) | N = live records. Pages read sequentially; no buffering. |
| `compact` | **O(S)** per page | O(PAGE_SIZE) | S = slots per page; not called automatically. |
| `scan_compressed` | **O(N)** + compress | O(N) | Full scan then encode; linear in record count. |

**Insert amortised analysis:** a page holds `⌊(PAGE_SIZE − HEADER_SIZE) / (rec_size + SLOT_SIZE)⌋` records. When the current page fills, a new page is allocated in O(1). Over R inserts: `R` page writes + `⌈R / capacity⌉` page allocations = O(R) total = O(1) per insert amortised.

---

## 15. Known Limitations

| Limitation | Impact |
|-----------|--------|
| No WAL / transaction log | Multi-page operations (e.g. overflow update spanning two pages) are not atomic at crash time. |
| No page compaction on delete | `fss` never retreats; deleted record space is stranded until `Page.compact()` is called manually. The FSM entry for a page does not grow after deletes. |
| FSM does not account for tombstone space | A page may report `free_bytes = 0` even if many records have been deleted; no new inserts will go there until a vacuum compacts it. |
| No buffer pool | Every page access opens and closes the file. For high-throughput workloads, an LRU buffer cache would be needed. |
| Forward chain depth ≤ 16 | Pathological workloads that overflow-update the same record more than 16 times in a row will raise a `RuntimeError`. |
| Dictionary encoder limited to uint16 codes | Maximum 65 535 distinct TEXT values per column batch. |
| No multi-process concurrency | No file locking; simultaneous writers from different processes will corrupt state. |

---

## 16. Module Map

```
storage.py
│
├── Constants & struct formats
│     PAGE_SIZE=4096, HEADER_SIZE=16, SLOT_SIZE=4, MIN_RECORD_SIZE=8
│     FORWARD_MARKER=0xFFFE, DELETED_OFFSET=0x0000, MOVED_FLAG=0x8000
│     _HEADER_FMT='>IHHHHI', _SLOT_FMT='>HH'
│     T_INT, T_BIGINT, T_FLOAT, T_TEXT, T_BOOL
│
├── RID  (NamedTuple: page_id, slot_id)
│
├── ColumnDef  (dataclass: name, col_type)
│
├── Page
│     __init__(page_id, data?)      ← fresh or from bytes
│     .page_id / .num_slots / .fss / .fse / .free_bytes  [read-only props]
│     .write_record(data, moved?)   → slot_id
│     .read_record(slot_id)         → bytes | None
│     .update_inplace(slot_id, data) → bool
│     .delete_record(slot_id)       → bool
│     .write_forward(slot_id, pg, sl)
│     .read_forward_target(slot_id) → RID
│     .is_forward / .is_deleted / .is_moved_target  (slot_id) → bool
│     .compact()                    → Page  (new, defragmented)
│     .as_bytes()                   → bytes
│
├── HeapFile
│     __init__(path)
│     .page_count                   [property]
│     .read_page(page_id)           → Page
│     .write_page(page)             [fsync]
│     .allocate_page()              → Page
│     .delete_file()
│
├── encode_record(schema, record)   → bytes
├── decode_record(schema, data)     → dict
│
├── Catalog
│     __init__(path)
│     .create_table / .drop_table / .table_exists / .list_tables
│     .get_schema(name)             → List[ColumnDef]
│     .get_fsm / .update_fsm
│     ._save()                      [atomic write-tmp-rename]
│
├── TableHandle
│     insert(record)                → RID
│     get(rid)                      → dict | None
│     update(rid, record)           → bool
│     delete(rid)                   → bool
│     scan()                        → Iterator[(RID, dict)]
│     count() / page_count()
│
├── StorageEngine
│     __init__(data_dir)
│     create_table / drop_table / open_table / list_tables / table_exists
│
└── Compression
      RLEEncoder.encode / decode / ratio
      DictEncoder.encode / decode / ratio
      scan_compressed(table, col_name) → bytes
```
