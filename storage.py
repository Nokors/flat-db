"""
storage.py — Flat-DB Storage Engine  (Part 2 · Track A: OLTP Row-Store)
========================================================================

Physical design overview
------------------------
Track A: slotted heap pages (fixed 4 KB), one file per table, free-space
map (FSM) kept in the per-table section of ``catalog.json``.

On-disk files
~~~~~~~~~~~~~
  <data_dir>/<table>.db      heap file – a contiguous sequence of 4 KB pages
  <data_dir>/catalog.json    schema + FSM for every table (atomic JSON writes)

Page layout  (PAGE_SIZE = 4096 bytes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  Byte 0-15  : Header (16 bytes)
  Byte 16+   : Record data, growing toward *higher* addresses  →
               ··· free space ···
               Slot directory, growing toward *lower* addresses  ←
  Byte 4095  : last byte of slot directory

  Header (big-endian):
    page_id   uint32   4 B   persistent page number
    num_slots uint16   2 B   total allocated slot entries (live + deleted)
    fss       uint16   2 B   free_space_start – next write offset (data grows →)
    fse       uint16   2 B   free_space_end   – first byte of slot dir (dir  grows ←)
    flags     uint16   2 B   reserved / future use
    reserved  uint32   4 B

  Slot entry (4 bytes, indexed from the END of the page, growing backward):
    offset  uint16   byte offset of record within the page
    length  uint16   byte length of the record (with flag bit)

  Slot sentinel values:
    offset == 0x0000, length == 0x0000   → deleted tombstone
    offset == 0xFFFE, length == <off>    → forwarding pointer;
                                           8-byte target RID is stored at
                                           byte <off> within the page
    length  &  0x8000 (MOVED_FLAG)       → record was relocated here by an
                                           overflow update; real length =
                                           length & 0x7FFF.  Scanner skips
                                           these slots (they are reachable
                                           only via a forwarding pointer).

Record encoding  (variable-length, big-endian)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  [null_bitmap: ⌈ncols/8⌉ bytes]
  For each column i in schema order, if NOT null:
    INT    → 4-byte signed  (struct '>i')
    BIGINT → 8-byte signed  (struct '>q')
    FLOAT  → 8-byte double  (struct '>d')
    TEXT   → uint16 length prefix (2 B) + UTF-8 bytes
    BOOL   → 1 byte (0 or 1)

  All records are zero-padded to MIN_RECORD_SIZE (8 bytes) so that
  forwarding pointers can always overwrite the first 8 bytes in place.

RID semantics
~~~~~~~~~~~~~
  RID = (page_id: int, slot_id: int), both 0-based.
  A RID is stable for the lifetime of a record as long as in-place
  updates succeed.  When an update requires more space than the old
  record, the old slot becomes a forwarding pointer and the original
  RID continues to resolve to the correct (new) data.  Callers always
  use the original RID; the engine follows the chain transparently.

Restart / recovery
~~~~~~~~~~~~~~~~~~
  Every page write is followed by flush + fsync.  The catalog (schema +
  FSM) is written atomically via a write-to-tmp-then-rename pattern.
  There is no WAL; the engine is crash-safe at single-page granularity.
  On restart, ``StorageEngine(data_dir)`` loads ``catalog.json`` and
  reopens all table heap files at the correct page count.

Complexity
~~~~~~~~~~
  insert  O(1) amortised – FSM lookup in catalog; O(P) worst case
  get     O(1) – direct page seek + slot lookup; O(1) per forward hop
  update  O(1) in-place; O(1) + new-insert if overflow
  delete  O(1) – tombstone write
  scan    O(N) – sequential page + slot traversal
"""

import io
import os
import json
import math
import struct
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, NamedTuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PAGE_SIZE       = 4096
HEADER_SIZE     = 16
SLOT_SIZE       = 4
MIN_RECORD_SIZE = 8     # guarantees forwarding-pointer overwrite is safe

FORWARD_MARKER  = 0xFFFE  # slot.offset sentinel: "this slot is a forward ptr"
DELETED_OFFSET  = 0x0000  # slot.offset sentinel: "this slot is deleted"
MOVED_FLAG      = 0x8000  # bit 15 of slot.length: two uses –
                          #   on a live slot: record was relocated here (scan skips it)
                          #   on a forward-pointer slot: the slot was a moved-target
                          #     before being re-forwarded (scan must also skip it)

# Header struct (big-endian): page_id(4), num_slots(2), fss(2), fse(2), flags(2), reserved(4)
_HEADER_FMT = '>IHHHHI'
_SLOT_FMT   = '>HH'

assert struct.calcsize(_HEADER_FMT) == HEADER_SIZE
assert struct.calcsize(_SLOT_FMT)   == SLOT_SIZE

# Column types
T_INT    = 'INT'
T_BIGINT = 'BIGINT'
T_FLOAT  = 'FLOAT'
T_TEXT   = 'TEXT'
T_BOOL   = 'BOOL'

VALID_TYPES = {T_INT, T_BIGINT, T_FLOAT, T_TEXT, T_BOOL}


# ---------------------------------------------------------------------------
# RID
# ---------------------------------------------------------------------------

class RID(NamedTuple):
    page_id: int
    slot_id: int

    def __repr__(self) -> str:
        return f"RID({self.page_id},{self.slot_id})"


# ---------------------------------------------------------------------------
# ColumnDef
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    name: str
    col_type: str  # one of VALID_TYPES

    def to_dict(self) -> dict:
        return {'name': self.name, 'type': self.col_type}

    @staticmethod
    def from_dict(d: dict) -> 'ColumnDef':
        return ColumnDef(d['name'], d['type'])


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

class Page:
    """In-memory representation of a single 4 KB slotted page.

    The page buffer is a mutable bytearray.  All mutating methods update
    the in-memory buffer; callers are responsible for writing the page
    back to disk via ``HeapFile.write_page()``.
    """

    __slots__ = ('_buf',)

    def __init__(self, page_id: int, data: Optional[bytes] = None):
        if data is not None:
            if len(data) != PAGE_SIZE:
                raise ValueError(
                    f"Page data must be exactly {PAGE_SIZE} bytes, got {len(data)}"
                )
            self._buf = bytearray(data)
        else:
            self._buf = bytearray(PAGE_SIZE)
            struct.pack_into(
                _HEADER_FMT, self._buf, 0,
                page_id,       # page_id
                0,             # num_slots
                HEADER_SIZE,   # fss: data starts right after header
                PAGE_SIZE,     # fse: slot directory starts at end
                0,             # flags
                0,             # reserved
            )

    # ------------------------------------------------------------------ header

    @property
    def page_id(self) -> int:
        return struct.unpack_from('>I', self._buf, 0)[0]

    @property
    def num_slots(self) -> int:
        return struct.unpack_from('>H', self._buf, 4)[0]

    @property
    def fss(self) -> int:
        """free_space_start: byte offset just past the last written record."""
        return struct.unpack_from('>H', self._buf, 6)[0]

    @property
    def fse(self) -> int:
        """free_space_end: byte offset of the first slot-dir entry."""
        return struct.unpack_from('>H', self._buf, 8)[0]

    @property
    def free_bytes(self) -> int:
        """Contiguous free bytes between the data region and the slot dir."""
        return self.fse - self.fss

    def _set_header(self, num_slots: int, fss: int, fse: int) -> None:
        struct.pack_into('>HHH', self._buf, 4, num_slots, fss, fse)

    # ------------------------------------------------------------------ slots

    def _slot_buf_offset(self, slot_id: int) -> int:
        """Byte offset of slot_id's 4-byte entry in the page buffer."""
        return PAGE_SIZE - (slot_id + 1) * SLOT_SIZE

    def get_slot(self, slot_id: int) -> Tuple[int, int]:
        """Return (offset, length) for slot_id."""
        return struct.unpack_from(_SLOT_FMT, self._buf, self._slot_buf_offset(slot_id))

    def set_slot(self, slot_id: int, offset: int, length: int) -> None:
        struct.pack_into(_SLOT_FMT, self._buf, self._slot_buf_offset(slot_id), offset, length)

    def _find_reusable_slot(self) -> Optional[int]:
        """Return index of first deleted slot (offset==0, length==0), or None."""
        for i in range(self.num_slots):
            off, ln = self.get_slot(i)
            if off == DELETED_OFFSET and ln == 0:
                return i
        return None

    # ------------------------------------------------------------------ I/O

    def write_record(self, data: bytes, moved: bool = False) -> int:
        """Append *data* to the data region and return the assigned slot_id.

        If *moved* is True the MOVED_FLAG bit is set on the slot length so
        that the scanner knows this record was relocated here (it will be
        returned only when following a forwarding pointer).

        Raises ``ValueError`` if the page does not have enough free space.
        """
        reuse = self._find_reusable_slot()
        new_slot = reuse is None             # True iff we must grow the slot dir
        needed   = len(data) + (SLOT_SIZE if new_slot else 0)
        if self.free_bytes < needed:
            raise ValueError(
                f"Page {self.page_id}: need {needed} B, have {self.free_bytes} B free"
            )
        slot_id    = reuse if reuse is not None else self.num_slots
        rec_offset = self.fss

        self._buf[rec_offset: rec_offset + len(data)] = data

        raw_len = len(data) | (MOVED_FLAG if moved else 0)
        self.set_slot(slot_id, rec_offset, raw_len)

        new_fss = self.fss + len(data)
        new_fse = self.fse - (SLOT_SIZE if new_slot else 0)
        new_num = self.num_slots + (1 if new_slot else 0)
        self._set_header(new_num, new_fss, new_fse)
        return slot_id

    def read_record(self, slot_id: int) -> Optional[bytes]:
        """Return raw record bytes for slot_id, or None if deleted / forward ptr."""
        if slot_id >= self.num_slots:
            raise IndexError(f"slot_id {slot_id} >= num_slots {self.num_slots}")
        off, ln = self.get_slot(slot_id)
        if off == DELETED_OFFSET and (ln & 0x7FFF) == 0:
            return None
        if off == FORWARD_MARKER:
            return None
        real_len = ln & 0x7FFF
        return bytes(self._buf[off: off + real_len])

    def update_inplace(self, slot_id: int, data: bytes) -> bool:
        """Overwrite slot_id's record in-place if *data* fits.  Returns True on success."""
        if slot_id >= self.num_slots:
            return False
        off, ln = self.get_slot(slot_id)
        if off == DELETED_OFFSET and (ln & 0x7FFF) == 0:
            return False
        if off == FORWARD_MARKER:
            return False
        real_len = ln & 0x7FFF
        if len(data) > real_len:
            return False
        self._buf[off: off + len(data)] = data
        # Preserve MOVED_FLAG if set: the forward chain still exists.
        self.set_slot(slot_id, off, len(data) | (ln & MOVED_FLAG))
        return True

    def delete_record(self, slot_id: int) -> bool:
        """Tombstone slot_id.  Returns True if the slot was live."""
        if slot_id >= self.num_slots:
            return False
        off, ln = self.get_slot(slot_id)
        if off == DELETED_OFFSET and (ln & 0x7FFF) == 0:
            return False  # already dead
        real_len = ln & 0x7FFF
        if off != FORWARD_MARKER and real_len > 0:
            # Zero out old record bytes (defensive; not strictly required)
            self._buf[off: off + real_len] = bytes(real_len)
        self.set_slot(slot_id, DELETED_OFFSET, 0)
        return True

    def is_forward(self, slot_id: int) -> bool:
        off, _ = self.get_slot(slot_id)
        return off == FORWARD_MARKER

    def is_deleted(self, slot_id: int) -> bool:
        off, ln = self.get_slot(slot_id)
        return off == DELETED_OFFSET and (ln & 0x7FFF) == 0

    def is_moved_target(self, slot_id: int) -> bool:
        """True for live records relocated here, or re-forwarded moved targets.

        Either way the scanner must skip this slot (it is reachable only
        through a forward-pointer chain).
        """
        _, ln = self.get_slot(slot_id)
        return bool(ln & MOVED_FLAG)

    def write_forward(self, slot_id: int, target_page: int, target_slot: int) -> None:
        """Convert slot_id into a forwarding pointer to (target_page, target_slot).

        The 8-byte target RID is written at the slot's old record offset so
        no additional space is consumed.  The ``length`` field of the slot
        is repurposed to store that data offset (it is >= HEADER_SIZE, which
        is an impossible real-length value for a slot, so it is unambiguous).

        If the slot being forwarded already had MOVED_FLAG (it was a moved
        target being re-forwarded), the MOVED_FLAG is preserved in the length
        field so that ``is_moved_target()`` still returns True and the scanner
        continues to skip this slot.
        """
        off, ln = self.get_slot(slot_id)
        if off == DELETED_OFFSET and (ln & 0x7FFF) == 0:
            raise ValueError("Cannot forward a deleted slot")

        if off == FORWARD_MARKER:
            # Re-forwarding an existing forward pointer: the data offset is in ln.
            data_off     = ln & 0x7FFF
            was_moved    = bool(ln & MOVED_FLAG)
            fwd_data     = struct.pack('>II', target_page, target_slot)
            self._buf[data_off: data_off + 8] = fwd_data
            self.set_slot(slot_id, FORWARD_MARKER, data_off | (MOVED_FLAG if was_moved else 0))
            return

        real_len = ln & 0x7FFF
        if real_len < MIN_RECORD_SIZE:
            # Should not happen because write_record pads to MIN_RECORD_SIZE
            raise RuntimeError(
                f"Record at slot {slot_id} is only {real_len} bytes; "
                "cannot store 8-byte forwarding RID"
            )
        was_moved = bool(ln & MOVED_FLAG)
        fwd_data  = struct.pack('>II', target_page, target_slot)
        self._buf[off: off + 8] = fwd_data
        # length field stores the data offset; MOVED_FLAG propagated if set
        self.set_slot(slot_id, FORWARD_MARKER, off | (MOVED_FLAG if was_moved else 0))

    def read_forward_target(self, slot_id: int) -> RID:
        """Return the RID stored in the forwarding pointer of slot_id."""
        _, ln    = self.get_slot(slot_id)
        fwd_off  = ln & 0x7FFF  # strip MOVED_FLAG from the data-offset field
        pg, sl   = struct.unpack_from('>II', self._buf, fwd_off)
        return RID(pg, sl)

    def as_bytes(self) -> bytes:
        return bytes(self._buf)

    def compact(self) -> 'Page':
        """Return a *new* page with all live records tightly packed.

        Only live, non-moved records are copied; deleted and forwarding-pointer
        slots are dropped.  The returned page has the same page_id and a fresh
        slot numbering (0, 1, …).  Callers that care about slot stability must
        handle the slot_id remapping externally.
        """
        new_page = Page(self.page_id)
        for slot_id in range(self.num_slots):
            if self.is_deleted(slot_id) or self.is_forward(slot_id):
                continue
            if self.is_moved_target(slot_id):
                continue
            data = self.read_record(slot_id)
            if data is not None:
                new_page.write_record(data)
        return new_page


# ---------------------------------------------------------------------------
# HeapFile
# ---------------------------------------------------------------------------

class HeapFile:
    """Manages a binary file of fixed-size pages.

    Pages are read / written individually; the file is opened and closed
    per operation so no file handle is held across calls.
    """

    def __init__(self, path: str) -> None:
        self._path   = path
        if os.path.exists(path):
            size = os.path.getsize(path)
            self._page_count = size // PAGE_SIZE
        else:
            self._page_count = 0

    @property
    def page_count(self) -> int:
        return self._page_count

    def _ensure_file(self) -> None:
        if not os.path.exists(self._path):
            with open(self._path, 'wb'):
                pass

    def read_page(self, page_id: int) -> Page:
        if page_id >= self._page_count:
            raise IndexError(f"Page {page_id} does not exist (page_count={self._page_count})")
        with open(self._path, 'rb') as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
        if len(data) != PAGE_SIZE:
            raise IOError(f"Short read on page {page_id}")
        return Page(page_id, data)

    def write_page(self, page: Page) -> None:
        """Flush the page to disk.  Creates the file if it does not exist.

        A new page (page_id == page_count) extends the file; pages beyond
        page_count + 1 are not allowed (would create a gap).
        """
        pid = page.page_id
        if pid > self._page_count:
            raise IndexError(
                f"Cannot write page {pid}: would create a gap "
                f"(current page_count={self._page_count})"
            )
        self._ensure_file()
        with open(self._path, 'r+b') as f:
            f.seek(pid * PAGE_SIZE)
            f.write(page.as_bytes())
            f.flush()
            os.fsync(f.fileno())
        if pid == self._page_count:
            self._page_count += 1

    def allocate_page(self) -> Page:
        """Create a new empty page at the end of the file and return it."""
        page = Page(self._page_count)
        self.write_page(page)
        return page

    def delete_file(self) -> None:
        if os.path.exists(self._path):
            os.remove(self._path)


# ---------------------------------------------------------------------------
# Record encoding / decoding
# ---------------------------------------------------------------------------

def encode_record(schema: List[ColumnDef], record: Dict[str, Any]) -> bytes:
    """Encode a record dict into raw bytes according to *schema*.

    Layout: [null_bitmap] [field_0_bytes] [field_1_bytes] …

    The result is zero-padded to ``MIN_RECORD_SIZE`` bytes so that
    forwarding pointers can always be written in-place.
    """
    n            = len(schema)
    bitmap_bytes = math.ceil(n / 8)
    bitmap       = bytearray(bitmap_bytes)
    parts: List[bytes] = []

    for i, col in enumerate(schema):
        val = record.get(col.name)
        if val is None:
            bitmap[i // 8] |= (1 << (i % 8))
            continue
        t = col.col_type
        if t == T_INT:
            parts.append(struct.pack('>i', int(val)))
        elif t == T_BIGINT:
            parts.append(struct.pack('>q', int(val)))
        elif t == T_FLOAT:
            parts.append(struct.pack('>d', float(val)))
        elif t == T_BOOL:
            parts.append(struct.pack('>B', 1 if val else 0))
        elif t == T_TEXT:
            enc = str(val).encode('utf-8')
            parts.append(struct.pack('>H', len(enc)) + enc)
        else:
            raise ValueError(f"Unknown column type: {t!r}")

    result = bytes(bitmap) + b''.join(parts)
    if len(result) < MIN_RECORD_SIZE:
        result = result + bytes(MIN_RECORD_SIZE - len(result))
    return result


def decode_record(schema: List[ColumnDef], data: bytes) -> Dict[str, Any]:
    """Decode raw bytes into a record dict according to *schema*."""
    n            = len(schema)
    bitmap_bytes = math.ceil(n / 8)
    bitmap       = data[:bitmap_bytes]
    offset       = bitmap_bytes
    record: Dict[str, Any] = {}

    for i, col in enumerate(schema):
        if bitmap[i // 8] & (1 << (i % 8)):
            record[col.name] = None
            continue
        t = col.col_type
        if t == T_INT:
            record[col.name] = struct.unpack_from('>i', data, offset)[0]
            offset += 4
        elif t == T_BIGINT:
            record[col.name] = struct.unpack_from('>q', data, offset)[0]
            offset += 8
        elif t == T_FLOAT:
            record[col.name] = struct.unpack_from('>d', data, offset)[0]
            offset += 8
        elif t == T_BOOL:
            record[col.name] = bool(struct.unpack_from('>B', data, offset)[0])
            offset += 1
        elif t == T_TEXT:
            ln = struct.unpack_from('>H', data, offset)[0]
            offset += 2
            record[col.name] = data[offset: offset + ln].decode('utf-8')
            offset += ln
        else:
            raise ValueError(f"Unknown column type: {t!r}")
    return record


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

class Catalog:
    """Persistent schema + free-space-map store, serialised as JSON.

    Every mutation is written atomically: the new JSON is written to a
    ``.tmp`` file first, then renamed over the original.  A partial write
    therefore never corrupts the existing catalog.

    Structure of catalog.json::

        {
          "tables": {
            "<name>": {
              "schema": [{"name": …, "type": …}, …],
              "page_count": <int>,
              "fsm": [<free_bytes_page0>, <free_bytes_page1>, …]
            }
          }
        }
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._data: Dict[str, Any] = {'tables': {}}
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                self._data = json.load(f)

    def _save(self) -> None:
        tmp = self._path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp, self._path)

    # ------------------------------------------------------------------ tables

    def create_table(self, name: str, schema: List[ColumnDef]) -> None:
        if name in self._data['tables']:
            raise ValueError(f"Table {name!r} already exists")
        self._data['tables'][name] = {
            'schema':     [c.to_dict() for c in schema],
            'page_count': 0,
            'fsm':        [],
        }
        self._save()

    def drop_table(self, name: str) -> None:
        if name not in self._data['tables']:
            raise KeyError(f"Table {name!r} does not exist")
        del self._data['tables'][name]
        self._save()

    def table_exists(self, name: str) -> bool:
        return name in self._data['tables']

    def list_tables(self) -> List[str]:
        return list(self._data['tables'].keys())

    def get_schema(self, name: str) -> List[ColumnDef]:
        raw = self._data['tables'][name]['schema']
        return [ColumnDef.from_dict(c) for c in raw]

    def get_fsm(self, name: str) -> List[int]:
        return self._data['tables'][name]['fsm']

    def get_page_count(self, name: str) -> int:
        return self._data['tables'][name]['page_count']

    def update_fsm(self, name: str, page_id: int, free_bytes: int) -> None:
        fsm = self._data['tables'][name]['fsm']
        while len(fsm) <= page_id:
            fsm.append(0)
        fsm[page_id] = free_bytes
        self._data['tables'][name]['page_count'] = len(fsm)
        self._save()


# ---------------------------------------------------------------------------
# TableHandle
# ---------------------------------------------------------------------------

class TableHandle:
    """Provides insert / get / update / delete / scan on a single table.

    Obtained via ``StorageEngine.open_table()``.
    """

    def __init__(self, engine: 'StorageEngine', name: str) -> None:
        self._engine = engine
        self._name   = name
        self._schema = engine._catalog.get_schema(name)
        self._heap   = HeapFile(engine._table_path(name))

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> List[ColumnDef]:
        return list(self._schema)

    # ------------------------------------------------------------------ insert

    def insert(self, record: Dict[str, Any], _moved: bool = False) -> RID:
        """Insert a new record and return its RID.

        *_moved* is an internal flag used by overflow-update to mark the
        new slot with MOVED_FLAG.
        """
        data   = encode_record(self._schema, record)
        needed = len(data) + SLOT_SIZE

        # Find a page with enough free space using the FSM
        fsm            = self._engine._catalog.get_fsm(self._name)
        target_page_id: Optional[int] = None
        for pid, free in enumerate(fsm):
            if free >= needed:
                target_page_id = pid
                break

        if target_page_id is None:
            # Allocate a new page
            page           = self._heap.allocate_page()
            target_page_id = page.page_id
        else:
            page = self._heap.read_page(target_page_id)

        slot_id = page.write_record(data, moved=_moved)
        self._heap.write_page(page)
        self._engine._catalog.update_fsm(self._name, target_page_id, page.free_bytes)
        return RID(target_page_id, slot_id)

    # ------------------------------------------------------------------ get

    def get(self, rid: RID) -> Optional[Dict[str, Any]]:
        """Return the record at *rid*, following forwarding pointers.

        Returns ``None`` if the record has been deleted.
        """
        return self._get_chain(rid, depth=0)

    def _get_chain(self, rid: RID, depth: int) -> Optional[Dict[str, Any]]:
        if depth > 16:
            raise RuntimeError("Forwarding-pointer chain depth exceeded 16")
        try:
            page = self._heap.read_page(rid.page_id)
        except IndexError:
            return None
        if rid.slot_id >= page.num_slots:
            return None
        if page.is_deleted(rid.slot_id):
            return None
        if page.is_forward(rid.slot_id):
            return self._get_chain(page.read_forward_target(rid.slot_id), depth + 1)
        data = page.read_record(rid.slot_id)
        return None if data is None else decode_record(self._schema, data)

    # ------------------------------------------------------------------ update

    def update(self, rid: RID, new_record: Dict[str, Any]) -> bool:
        """Update the record at *rid*.

        Tries in-place first.  If the new record is larger than the old slot,
        inserts at a new location and converts the original slot to a
        forwarding pointer.  Returns False if the record does not exist.
        """
        return self._update_chain(rid, new_record, depth=0)

    def _update_chain(
        self, rid: RID, new_record: Dict[str, Any], depth: int
    ) -> bool:
        if depth > 16:
            raise RuntimeError("Forwarding-pointer chain depth exceeded 16")
        page = self._heap.read_page(rid.page_id)
        if rid.slot_id >= page.num_slots:
            return False
        if page.is_deleted(rid.slot_id):
            return False
        if page.is_forward(rid.slot_id):
            target = page.read_forward_target(rid.slot_id)
            return self._update_chain(target, new_record, depth + 1)

        new_data = encode_record(self._schema, new_record)

        if page.update_inplace(rid.slot_id, new_data):
            self._heap.write_page(page)
            self._engine._catalog.update_fsm(self._name, rid.page_id, page.free_bytes)
            return True

        # Overflow: insert at a new location, mark as moved, then forward
        new_rid = self.insert(new_record, _moved=True)

        # Re-read the original page (insert might have modified it if same page)
        page = self._heap.read_page(rid.page_id)
        page.write_forward(rid.slot_id, new_rid.page_id, new_rid.slot_id)
        self._heap.write_page(page)
        # free_bytes of rid.page_id is unchanged by write_forward
        self._engine._catalog.update_fsm(self._name, rid.page_id, page.free_bytes)
        return True

    # ------------------------------------------------------------------ delete

    def delete(self, rid: RID) -> bool:
        """Delete the record at *rid*.  Returns False if already deleted / missing."""
        return self._delete_chain(rid, depth=0)

    def _delete_chain(self, rid: RID, depth: int) -> bool:
        if depth > 16:
            raise RuntimeError("Forwarding-pointer chain depth exceeded 16")
        page = self._heap.read_page(rid.page_id)
        if rid.slot_id >= page.num_slots:
            return False
        if page.is_deleted(rid.slot_id):
            return False

        if page.is_forward(rid.slot_id):
            target = page.read_forward_target(rid.slot_id)
            # Delete the actual data at the target first
            self._delete_chain(target, depth + 1)
            # Then tombstone the forwarding-pointer slot
            page = self._heap.read_page(rid.page_id)
            page.delete_record(rid.slot_id)
            self._heap.write_page(page)
            self._engine._catalog.update_fsm(self._name, rid.page_id, page.free_bytes)
            return True

        result = page.delete_record(rid.slot_id)
        if result:
            self._heap.write_page(page)
            self._engine._catalog.update_fsm(self._name, rid.page_id, page.free_bytes)
        return result

    # ------------------------------------------------------------------ scan

    def scan(self) -> Iterator[Tuple[RID, Dict[str, Any]]]:
        """Yield (RID, record) for every live record in the table.

        Records reached via forwarding pointers are yielded with the
        *original* RID (the caller's stable handle).  Slots marked with
        MOVED_FLAG are skipped (they are surfaced only through their
        originating forwarding pointer).
        """
        for page_id in range(self._heap.page_count):
            page = self._heap.read_page(page_id)
            for slot_id in range(page.num_slots):
                rid = RID(page_id, slot_id)
                if page.is_deleted(slot_id):
                    continue
                if page.is_moved_target(slot_id):
                    # Reachable only via a forward chain; skip direct exposure
                    continue
                if page.is_forward(slot_id):
                    # Follow the chain; return with original RID
                    record = self._get_chain(rid, depth=0)
                    if record is not None:
                        yield rid, record
                    continue
                data = page.read_record(slot_id)
                if data is not None:
                    yield rid, decode_record(self._schema, data)

    # ------------------------------------------------------------------ util

    def count(self) -> int:
        """Return the number of live records (full scan)."""
        return sum(1 for _ in self.scan())

    def page_count(self) -> int:
        return self._heap.page_count


# ---------------------------------------------------------------------------
# StorageEngine
# ---------------------------------------------------------------------------

class StorageEngine:
    """Top-level API: manages multiple tables in a data directory.

    Usage::

        engine = StorageEngine("./data")
        engine.create_table("users", [
            ColumnDef("id",    T_INT),
            ColumnDef("name",  T_TEXT),
            ColumnDef("score", T_FLOAT),
        ])
        tbl = engine.open_table("users")
        rid = tbl.insert({"id": 1, "name": "Alice", "score": 9.5})
        print(tbl.get(rid))
    """

    def __init__(self, data_dir: str = 'data') -> None:
        self._data_dir     = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._catalog      = Catalog(os.path.join(data_dir, 'catalog.json'))
        self._open_tables: Dict[str, TableHandle] = {}

    def _table_path(self, name: str) -> str:
        return os.path.join(self._data_dir, f'{name}.db')

    # ------------------------------------------------------------------ DDL

    def create_table(
        self,
        table_name: str,
        schema: List[Dict[str, str]],
    ) -> None:
        """Create a new table.

        *schema* is a list of ``{"name": …, "type": …}`` dicts, e.g.::

            [{"name": "id", "type": "INT"}, {"name": "name", "type": "TEXT"}]
        """
        cols: List[ColumnDef] = []
        for c in schema:
            if c['type'] not in VALID_TYPES:
                raise ValueError(
                    f"Unknown column type {c['type']!r}. "
                    f"Valid types: {sorted(VALID_TYPES)}"
                )
            cols.append(ColumnDef(c['name'], c['type']))
        self._catalog.create_table(table_name, cols)

    def drop_table(self, table_name: str) -> None:
        """Drop a table and delete its heap file."""
        self._open_tables.pop(table_name, None)
        self._catalog.drop_table(table_name)
        heap = HeapFile(self._table_path(table_name))
        heap.delete_file()

    def open_table(self, table_name: str) -> TableHandle:
        """Return a ``TableHandle`` for *table_name*.  Results are cached."""
        if not self._catalog.table_exists(table_name):
            raise KeyError(f"Table {table_name!r} does not exist")
        if table_name not in self._open_tables:
            self._open_tables[table_name] = TableHandle(self, table_name)
        return self._open_tables[table_name]

    def list_tables(self) -> List[str]:
        return self._catalog.list_tables()

    def table_exists(self, table_name: str) -> bool:
        return self._catalog.table_exists(table_name)


# ---------------------------------------------------------------------------
# Compression utilities  (advanced requirement)
# ---------------------------------------------------------------------------

class RLEEncoder:
    """Run-Length Encoding for integer columns (INT / BIGINT).

    Wire format::

        [uint32 run_count]
        [run_count × (value: int_bytes, count: uint32)]

    Works on Python int lists and round-trips through encode/decode.
    """

    @staticmethod
    def encode(values: List[int], col_type: str = T_INT) -> bytes:
        """Encode a list of integers into RLE bytes."""
        if not values:
            return struct.pack('>I', 0)
        fmt  = '>i' if col_type == T_INT else '>q'
        size = 4    if col_type == T_INT else 8

        runs: List[Tuple[int, int]] = []
        cur_val, cur_cnt = values[0], 1
        for v in values[1:]:
            if v == cur_val:
                cur_cnt += 1
            else:
                runs.append((cur_val, cur_cnt))
                cur_val, cur_cnt = v, 1
        runs.append((cur_val, cur_cnt))

        buf = struct.pack('>I', len(runs))
        for val, cnt in runs:
            buf += struct.pack(fmt, val) + struct.pack('>I', cnt)
        return buf

    @staticmethod
    def decode(data: bytes, col_type: str = T_INT) -> List[int]:
        """Decode RLE bytes back into a list of integers."""
        fmt  = '>i' if col_type == T_INT else '>q'
        size = 4    if col_type == T_INT else 8
        offset = 0
        n_runs = struct.unpack_from('>I', data, offset)[0]
        offset += 4
        result: List[int] = []
        for _ in range(n_runs):
            val = struct.unpack_from(fmt, data, offset)[0]
            offset += size
            cnt = struct.unpack_from('>I', data, offset)[0]
            offset += 4
            result.extend([val] * cnt)
        return result

    @staticmethod
    def ratio(original: List[int], col_type: str = T_INT) -> float:
        """Return compressed_size / original_size (lower = better)."""
        if not original:
            return 1.0
        size     = 4 if col_type == T_INT else 8
        orig_sz  = size * len(original)
        comp_sz  = len(RLEEncoder.encode(original, col_type))
        return comp_sz / orig_sz


class DictEncoder:
    """Dictionary encoding for TEXT columns.

    Assigns each unique string a uint16 code.  Good when cardinality is low.

    Wire format::

        [uint16 dict_size]
        [dict_size × (uint16 str_len, UTF-8 bytes)]
        [uint32 value_count]
        [value_count × uint16 code]
    """

    @staticmethod
    def encode(values: List[Optional[str]]) -> bytes:
        """Encode a list of strings (None → empty string) into dict bytes."""
        clean  = [v if v is not None else '' for v in values]
        unique = list(dict.fromkeys(clean))  # preserve insertion order
        code_of = {s: i for i, s in enumerate(unique)}

        buf = struct.pack('>H', len(unique))
        for s in unique:
            enc = s.encode('utf-8')
            buf += struct.pack('>H', len(enc)) + enc

        buf += struct.pack('>I', len(clean))
        for s in clean:
            buf += struct.pack('>H', code_of[s])
        return buf

    @staticmethod
    def decode(data: bytes) -> List[str]:
        """Decode dict bytes back into a list of strings."""
        offset    = 0
        dict_size = struct.unpack_from('>H', data, offset)[0]
        offset   += 2
        dictionary: List[str] = []
        for _ in range(dict_size):
            slen   = struct.unpack_from('>H', data, offset)[0]
            offset += 2
            dictionary.append(data[offset: offset + slen].decode('utf-8'))
            offset += slen

        n_vals = struct.unpack_from('>I', data, offset)[0]
        offset += 4
        result: List[str] = []
        for _ in range(n_vals):
            code   = struct.unpack_from('>H', data, offset)[0]
            offset += 2
            result.append(dictionary[code])
        return result

    @staticmethod
    def ratio(values: List[Optional[str]]) -> float:
        """Return compressed_size / original_size (lower = better)."""
        clean   = [v if v is not None else '' for v in values]
        orig_sz = sum(len(s.encode('utf-8')) + 2 for s in clean)  # +2 = length prefix
        if orig_sz == 0:
            return 1.0
        return len(DictEncoder.encode(values)) / orig_sz


def scan_compressed(
    table: TableHandle,
    col_name: str,
) -> bytes:
    """Scan *col_name* from *table* and return it in compressed form.

    INT / BIGINT columns use RLE; TEXT columns use dictionary encoding.
    Useful for bulk analytics on a row-store table.
    """
    schema = {c.name: c for c in table.schema}
    if col_name not in schema:
        raise KeyError(f"Column {col_name!r} not in table {table.name!r}")
    col = schema[col_name]
    values = [rec.get(col_name) for _, rec in table.scan()]

    if col.col_type in (T_INT, T_BIGINT):
        int_vals = [v if v is not None else 0 for v in values]
        return RLEEncoder.encode(int_vals, col.col_type)
    if col.col_type == T_TEXT:
        return DictEncoder.encode(values)
    raise TypeError(
        f"Compression not implemented for type {col.col_type!r}; "
        "supported: INT, BIGINT, TEXT"
    )
