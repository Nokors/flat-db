"""
test_storage.py — pytest test suite for the Flat-DB storage engine.

Run:
    pytest test_storage.py -v

Test groups
-----------
TestPage               – unit tests for the Page class in isolation
TestRecordEncoding     – encode_record / decode_record round-trips
TestHeapFile           – HeapFile page I/O
TestStorageCRUD        – insert / get / update / delete via StorageEngine
TestPersistence        – data survives an engine restart (new Python object)
TestEdgeCases          – page-full, varlen boundaries, tombstones, forward ptrs
TestMultiTable         – independent tables in the same data dir
TestCompression        – RLEEncoder, DictEncoder, scan_compressed
"""

import os
import shutil
import struct
import tempfile
import pytest

from storage import (
    PAGE_SIZE, HEADER_SIZE, SLOT_SIZE, MIN_RECORD_SIZE,
    FORWARD_MARKER, DELETED_OFFSET, MOVED_FLAG,
    T_INT, T_BIGINT, T_FLOAT, T_TEXT, T_BOOL,
    RID, ColumnDef, Page, HeapFile,
    encode_record, decode_record,
    Catalog, TableHandle, StorageEngine,
    RLEEncoder, DictEncoder, scan_compressed,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def tmp_dir():
    """Return a fresh temporary directory path (caller must clean up)."""
    return tempfile.mkdtemp(prefix='flatdb_test_')


USERS_SCHEMA = [
    {'name': 'id',    'type': 'INT'},
    {'name': 'name',  'type': 'TEXT'},
    {'name': 'score', 'type': 'FLOAT'},
]

ORDERS_SCHEMA = [
    {'name': 'order_id', 'type': 'BIGINT'},
    {'name': 'item',     'type': 'TEXT'},
    {'name': 'qty',      'type': 'INT'},
    {'name': 'shipped',  'type': 'BOOL'},
]

SAMPLE_USER = {'id': 1, 'name': 'Alice', 'score': 9.5}


# ---------------------------------------------------------------------------
# TestPage
# ---------------------------------------------------------------------------

class TestPage:

    def test_fresh_page_header(self):
        p = Page(0)
        assert p.page_id   == 0
        assert p.num_slots == 0
        assert p.fss       == HEADER_SIZE
        assert p.fse       == PAGE_SIZE
        assert p.free_bytes == PAGE_SIZE - HEADER_SIZE

    def test_page_id_preserved_in_bytes(self):
        p = Page(42)
        p2 = Page(42, p.as_bytes())
        assert p2.page_id == 42

    def test_write_and_read_record(self):
        p    = Page(0)
        data = b'hello world'
        sid  = p.write_record(data)
        assert sid == 0
        assert p.read_record(0) == data

    def test_multiple_records(self):
        p  = Page(0)
        d1 = b'AAAA' + bytes(4)  # 8 bytes (MIN_RECORD_SIZE)
        d2 = b'BBBB' + bytes(4)
        d3 = b'CCCC' + bytes(4)
        s1 = p.write_record(d1)
        s2 = p.write_record(d2)
        s3 = p.write_record(d3)
        assert (s1, s2, s3) == (0, 1, 2)
        assert p.read_record(s1) == d1
        assert p.read_record(s2) == d2
        assert p.read_record(s3) == d3

    def test_delete_tombstone(self):
        p   = Page(0)
        sid = p.write_record(b'X' * MIN_RECORD_SIZE)
        assert p.delete_record(sid)
        assert p.read_record(sid) is None
        assert p.is_deleted(sid)

    def test_delete_idempotent(self):
        p   = Page(0)
        sid = p.write_record(b'X' * MIN_RECORD_SIZE)
        assert p.delete_record(sid)
        assert not p.delete_record(sid)  # second delete returns False

    def test_reuse_deleted_slot(self):
        p  = Page(0)
        s0 = p.write_record(b'A' * MIN_RECORD_SIZE)
        p.delete_record(s0)
        s1 = p.write_record(b'B' * MIN_RECORD_SIZE)
        assert s1 == s0  # slot index reused

    def test_update_inplace_fits(self):
        p    = Page(0)
        data = b'hello' + bytes(3)  # 8 bytes
        sid  = p.write_record(data)
        new  = b'world' + bytes(3)
        assert p.update_inplace(sid, new)
        assert p.read_record(sid) == new

    def test_update_inplace_too_large(self):
        p    = Page(0)
        data = b'X' * MIN_RECORD_SIZE
        sid  = p.write_record(data)
        big  = b'Y' * (MIN_RECORD_SIZE + 1)
        assert not p.update_inplace(sid, big)

    def test_forward_pointer(self):
        p   = Page(0)
        sid = p.write_record(b'original_data_')
        p.write_forward(sid, target_page=7, target_slot=3)
        assert p.is_forward(sid)
        target = p.read_forward_target(sid)
        assert target == RID(7, 3)

    def test_free_bytes_accounting(self):
        p       = Page(0)
        initial = p.free_bytes
        data    = b'X' * 20
        p.write_record(data)
        assert p.free_bytes == initial - len(data) - SLOT_SIZE

    def test_page_full_raises(self):
        p = Page(0)
        # Fill up the page with large records
        chunk = b'F' * (PAGE_SIZE // 8)
        for _ in range(7):
            try:
                p.write_record(chunk)
            except ValueError:
                break
        with pytest.raises(ValueError, match="free"):
            # This should eventually fail
            for _ in range(1000):
                p.write_record(chunk)

    def test_moved_flag(self):
        p   = Page(0)
        sid = p.write_record(b'M' * MIN_RECORD_SIZE, moved=True)
        assert p.is_moved_target(sid)
        # The actual data is still readable
        assert p.read_record(sid) == b'M' * MIN_RECORD_SIZE

    def test_compact_removes_deleted(self):
        p   = Page(0)
        s0  = p.write_record(b'keep' + bytes(4))
        s1  = p.write_record(b'gone' + bytes(4))
        s2  = p.write_record(b'also' + bytes(4))
        p.delete_record(s1)
        new_page = p.compact()
        # Two live records, zero deleted
        live = [
            new_page.read_record(i)
            for i in range(new_page.num_slots)
            if not new_page.is_deleted(i) and not new_page.is_forward(i)
        ]
        assert len(live) == 2

    def test_round_trip_bytes(self):
        p   = Page(5)
        p.write_record(b'hello' + bytes(3))
        p.write_record(b'world' + bytes(3))
        p2 = Page(5, p.as_bytes())
        assert p2.num_slots == 2
        assert p2.read_record(0) == b'hello' + bytes(3)
        assert p2.read_record(1) == b'world' + bytes(3)


# ---------------------------------------------------------------------------
# TestRecordEncoding
# ---------------------------------------------------------------------------

class TestRecordEncoding:

    SCHEMA = [
        ColumnDef('id',      T_INT),
        ColumnDef('name',    T_TEXT),
        ColumnDef('score',   T_FLOAT),
        ColumnDef('active',  T_BOOL),
        ColumnDef('big_num', T_BIGINT),
    ]

    def _rt(self, record):
        """Encode then decode and return the result."""
        data = encode_record(self.SCHEMA, record)
        return decode_record(self.SCHEMA, data)

    def test_all_types_roundtrip(self):
        rec = {'id': 42, 'name': 'Alice', 'score': 3.14, 'active': True, 'big_num': 10**15}
        assert self._rt(rec) == rec

    def test_null_values(self):
        rec = {'id': 1, 'name': None, 'score': None, 'active': False, 'big_num': -99}
        rt  = self._rt(rec)
        assert rt['name']   is None
        assert rt['score']  is None
        assert rt['id']     == 1
        assert rt['active'] == False

    def test_all_nulls(self):
        rec = {c.name: None for c in self.SCHEMA}
        rt  = self._rt(rec)
        for k, v in rt.items():
            assert v is None

    def test_empty_string(self):
        rec = {'id': 0, 'name': '', 'score': 0.0, 'active': False, 'big_num': 0}
        assert self._rt(rec)['name'] == ''

    def test_unicode_text(self):
        rec = {'id': 1, 'name': 'Ångström ∞', 'score': 1.0, 'active': True, 'big_num': 0}
        assert self._rt(rec)['name'] == 'Ångström ∞'

    def test_negative_integers(self):
        rec = {'id': -2147483648, 'name': 'x', 'score': -1.0, 'active': False, 'big_num': -1}
        rt  = self._rt(rec)
        assert rt['id']     == -2147483648
        assert rt['big_num'] == -1

    def test_min_record_size_padding(self):
        # A schema with a single BOOL column produces a very small record
        schema = [ColumnDef('flag', T_BOOL)]
        data   = encode_record(schema, {'flag': True})
        assert len(data) >= MIN_RECORD_SIZE

    def test_large_text_field(self):
        long_str = 'x' * 2000
        schema   = [ColumnDef('blob', T_TEXT)]
        data     = encode_record(schema, {'blob': long_str})
        rt       = decode_record(schema, data)
        assert rt['blob'] == long_str

    def test_missing_key_treated_as_null(self):
        # If a key is absent from the record dict, it should encode as NULL
        schema = [ColumnDef('a', T_INT), ColumnDef('b', T_TEXT)]
        data   = encode_record(schema, {'a': 5})  # 'b' missing
        rt     = decode_record(schema, data)
        assert rt['a'] == 5
        assert rt['b'] is None


# ---------------------------------------------------------------------------
# TestHeapFile
# ---------------------------------------------------------------------------

class TestHeapFile:

    def setup_method(self):
        self.tmpdir = tmp_dir()

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _path(self):
        return os.path.join(self.tmpdir, 'test.db')

    def test_new_heap_file_has_zero_pages(self):
        hf = HeapFile(self._path())
        assert hf.page_count == 0

    def test_allocate_and_read_page(self):
        hf = HeapFile(self._path())
        p  = hf.allocate_page()
        assert p.page_id     == 0
        assert hf.page_count == 1
        p2 = hf.read_page(0)
        assert p2.page_id == 0

    def test_write_and_read_back(self):
        hf  = HeapFile(self._path())
        p   = hf.allocate_page()
        p.write_record(b'persistent' + bytes(MIN_RECORD_SIZE - 10 + 10))
        hf.write_page(p)
        # re-open
        hf2 = HeapFile(self._path())
        p2  = hf2.read_page(0)
        assert p2.read_record(0) is not None

    def test_multiple_pages(self):
        hf = HeapFile(self._path())
        for _ in range(5):
            hf.allocate_page()
        assert hf.page_count == 5
        hf2 = HeapFile(self._path())
        assert hf2.page_count == 5

    def test_page_id_gap_raises(self):
        hf = HeapFile(self._path())
        p  = Page(5)  # skip pages 0-4
        with pytest.raises(IndexError):
            hf.write_page(p)

    def test_delete_file(self):
        hf = HeapFile(self._path())
        hf.allocate_page()
        assert os.path.exists(self._path())
        hf.delete_file()
        assert not os.path.exists(self._path())


# ---------------------------------------------------------------------------
# TestStorageCRUD
# ---------------------------------------------------------------------------

class TestStorageCRUD:

    def setup_method(self):
        self.tmpdir = tmp_dir()
        self.engine = StorageEngine(self.tmpdir)
        self.engine.create_table('users', USERS_SCHEMA)
        self.tbl = self.engine.open_table('users')

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_insert_returns_rid(self):
        rid = self.tbl.insert(SAMPLE_USER)
        assert isinstance(rid, RID)

    def test_get_after_insert(self):
        rid = self.tbl.insert(SAMPLE_USER)
        rec = self.tbl.get(rid)
        assert rec == SAMPLE_USER

    def test_get_nonexistent_rid_returns_none(self):
        assert self.tbl.get(RID(99, 0)) is None

    def test_update_inplace(self):
        rid = self.tbl.insert(SAMPLE_USER)
        updated = {'id': 1, 'name': 'Bob', 'score': 7.0}
        assert self.tbl.update(rid, updated)
        assert self.tbl.get(rid) == updated

    def test_update_overflow_rid_stable(self):
        """When update moves the record, the original RID should still resolve."""
        rid = self.tbl.insert({'id': 1, 'name': 'A', 'score': 0.0})
        # Update with a much longer name to force overflow
        big_name = 'B' * 3000
        assert self.tbl.update(rid, {'id': 1, 'name': big_name, 'score': 0.0})
        rec = self.tbl.get(rid)
        assert rec is not None
        assert rec['name'] == big_name

    def test_delete(self):
        rid = self.tbl.insert(SAMPLE_USER)
        assert self.tbl.delete(rid)
        assert self.tbl.get(rid) is None

    def test_delete_idempotent(self):
        rid = self.tbl.insert(SAMPLE_USER)
        assert self.tbl.delete(rid)
        assert not self.tbl.delete(rid)

    def test_update_deleted_returns_false(self):
        rid = self.tbl.insert(SAMPLE_USER)
        self.tbl.delete(rid)
        assert not self.tbl.update(rid, SAMPLE_USER)

    def test_scan_empty_table(self):
        assert list(self.tbl.scan()) == []

    def test_scan_returns_all_live(self):
        rids = [self.tbl.insert({'id': i, 'name': f'u{i}', 'score': float(i)})
                for i in range(10)]
        results = list(self.tbl.scan())
        assert len(results) == 10

    def test_scan_skips_deleted(self):
        r0 = self.tbl.insert({'id': 0, 'name': 'del', 'score': 0.0})
        r1 = self.tbl.insert({'id': 1, 'name': 'live', 'score': 1.0})
        self.tbl.delete(r0)
        results = list(self.tbl.scan())
        assert len(results) == 1
        assert results[0][1]['name'] == 'live'

    def test_scan_after_overflow_update_no_duplicates(self):
        """After an overflow update, each record appears exactly once in a scan."""
        rid      = self.tbl.insert({'id': 1, 'name': 'short', 'score': 1.0})
        big_name = 'X' * 3000
        self.tbl.update(rid, {'id': 1, 'name': big_name, 'score': 1.0})
        results = list(self.tbl.scan())
        assert len(results) == 1
        assert results[0][1]['name'] == big_name

    def test_scan_after_double_overflow_update_no_duplicates(self):
        """Two consecutive overflow updates must not produce duplicate rows in scan."""
        rid = self.tbl.insert({'id': 1, 'name': 'v0', 'score': 0.0})
        self.tbl.update(rid, {'id': 1, 'name': 'X' * 2000, 'score': 1.0})
        self.tbl.update(rid, {'id': 1, 'name': 'Y' * 3000, 'score': 2.0})
        results = list(self.tbl.scan())
        assert len(results) == 1, f"Expected 1 result, got {len(results)}"
        assert results[0][1]['name'] == 'Y' * 3000

    def test_null_field_insert_and_get(self):
        rec = {'id': 5, 'name': None, 'score': 2.0}
        rid = self.tbl.insert(rec)
        assert self.tbl.get(rid) == rec

    def test_insert_many_spanning_pages(self):
        """Insert enough records to fill more than one page."""
        n = 200
        rids = [self.tbl.insert({'id': i, 'name': f'user_{i}', 'score': i * 0.1})
                for i in range(n)]
        assert self.tbl.page_count() >= 2
        # All records readable
        for i, rid in enumerate(rids):
            rec = self.tbl.get(rid)
            assert rec is not None
            assert rec['id'] == i

    def test_count(self):
        for i in range(5):
            self.tbl.insert({'id': i, 'name': 'u', 'score': 0.0})
        assert self.tbl.count() == 5

    def test_create_duplicate_table_raises(self):
        with pytest.raises(ValueError):
            self.engine.create_table('users', USERS_SCHEMA)

    def test_drop_table(self):
        self.engine.drop_table('users')
        assert not self.engine.table_exists('users')

    def test_open_nonexistent_table_raises(self):
        with pytest.raises(KeyError):
            self.engine.open_table('no_such_table')

    def test_list_tables(self):
        self.engine.create_table('orders', ORDERS_SCHEMA)
        tables = self.engine.list_tables()
        assert 'users'  in tables
        assert 'orders' in tables


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    """Verify that data survives engine restart (new StorageEngine object)."""

    def setup_method(self):
        self.tmpdir = tmp_dir()

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _new_engine(self):
        return StorageEngine(self.tmpdir)

    def test_schema_survives_restart(self):
        e1 = self._new_engine()
        e1.create_table('t', [{'name': 'x', 'type': 'INT'}])
        e2 = self._new_engine()
        assert e2.table_exists('t')
        schema = e2._catalog.get_schema('t')
        assert schema[0].name == 'x'
        assert schema[0].col_type == 'INT'

    def test_records_survive_restart(self):
        e1  = self._new_engine()
        e1.create_table('users', USERS_SCHEMA)
        t1  = e1.open_table('users')
        rid = t1.insert(SAMPLE_USER)

        e2  = self._new_engine()
        t2  = e2.open_table('users')
        rec = t2.get(rid)
        assert rec == SAMPLE_USER

    def test_delete_survives_restart(self):
        e1  = self._new_engine()
        e1.create_table('users', USERS_SCHEMA)
        t1  = e1.open_table('users')
        rid = t1.insert(SAMPLE_USER)
        t1.delete(rid)

        e2 = self._new_engine()
        t2 = e2.open_table('users')
        assert t2.get(rid) is None

    def test_update_survives_restart(self):
        e1  = self._new_engine()
        e1.create_table('users', USERS_SCHEMA)
        t1  = e1.open_table('users')
        rid = t1.insert(SAMPLE_USER)
        t1.update(rid, {'id': 1, 'name': 'Updated', 'score': 0.0})

        e2  = self._new_engine()
        t2  = e2.open_table('users')
        rec = t2.get(rid)
        assert rec['name'] == 'Updated'

    def test_overflow_update_survives_restart(self):
        e1      = self._new_engine()
        e1.create_table('users', USERS_SCHEMA)
        t1      = e1.open_table('users')
        rid     = t1.insert({'id': 1, 'name': 'short', 'score': 1.0})
        big_name = 'Z' * 3000
        t1.update(rid, {'id': 1, 'name': big_name, 'score': 1.0})

        e2  = self._new_engine()
        t2  = e2.open_table('users')
        rec = t2.get(rid)
        assert rec['name'] == big_name

    def test_many_records_survive_restart(self):
        e1  = self._new_engine()
        e1.create_table('users', USERS_SCHEMA)
        t1  = e1.open_table('users')
        n   = 100
        rids = [t1.insert({'id': i, 'name': f'user_{i}', 'score': i * 0.5})
                for i in range(n)]

        e2 = self._new_engine()
        t2 = e2.open_table('users')
        assert t2.count() == n
        for i, rid in enumerate(rids):
            rec = t2.get(rid)
            assert rec['id'] == i

    def test_drop_table_removes_file(self):
        e1 = self._new_engine()
        e1.create_table('tmp', [{'name': 'v', 'type': 'INT'}])
        t1 = e1.open_table('tmp')
        t1.insert({'v': 1})
        e1.drop_table('tmp')

        e2 = self._new_engine()
        assert not e2.table_exists('tmp')
        assert not os.path.exists(os.path.join(self.tmpdir, 'tmp.db'))


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def setup_method(self):
        self.tmpdir = tmp_dir()
        self.engine = StorageEngine(self.tmpdir)

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_varlen_boundary_exactly_page_size(self):
        """A TEXT value just large enough to require a second page."""
        self.engine.create_table('big', [{'name': 'blob', 'type': 'TEXT'}])
        tbl = self.engine.open_table('big')
        # A single record that barely fits in one page
        # PAGE_SIZE=4096, HEADER=16, SLOT=4 → 4076 bytes usable for 1 record
        # encode: 1 null-bitmap byte + 2 length prefix + data; pad to MIN_RECORD_SIZE
        # So max text per record ≈ 4076 - 3 = 4073 chars
        text = 'A' * 4070
        rid  = tbl.insert({'blob': text})
        assert tbl.get(rid)['blob'] == text

    def test_page_full_spills_to_new_page(self):
        """Records that don't fit on the current page go to a new page."""
        self.engine.create_table('spill', [{'name': 'v', 'type': 'INT'}])
        tbl   = self.engine.open_table('spill')
        rids  = [tbl.insert({'v': i}) for i in range(500)]
        pages = tbl.page_count()
        assert pages > 1
        for i, rid in enumerate(rids):
            assert tbl.get(rid)['v'] == i

    def test_delete_then_insert_reuses_slot(self):
        self.engine.create_table('slots', [{'name': 'n', 'type': 'INT'}])
        tbl = self.engine.open_table('slots')
        r0  = tbl.insert({'n': 10})
        tbl.delete(r0)
        r1  = tbl.insert({'n': 20})
        # Slot directory should reuse index 0
        assert r1.slot_id == 0
        assert tbl.get(r1)['n'] == 20

    def test_forward_pointer_chain_get(self):
        """Multiple consecutive overflow updates create a chain; get must follow it."""
        self.engine.create_table('chain', [{'name': 'txt', 'type': 'TEXT'}])
        tbl = self.engine.open_table('chain')
        rid = tbl.insert({'txt': 'v0'})
        tbl.update(rid, {'txt': 'X' * 2000})
        tbl.update(rid, {'txt': 'Y' * 3000})
        rec = tbl.get(rid)
        assert rec is not None
        assert rec['txt'] == 'Y' * 3000

    def test_delete_forwarded_record(self):
        """Deleting a record that was overflow-updated should succeed."""
        self.engine.create_table('fwddel', [{'name': 'v', 'type': 'TEXT'}])
        tbl = self.engine.open_table('fwddel')
        rid = tbl.insert({'v': 'small'})
        tbl.update(rid, {'v': 'X' * 3000})
        assert tbl.delete(rid)
        assert tbl.get(rid) is None

    def test_scan_over_many_pages_complete(self):
        """scan() must yield every live record, even across many pages."""
        self.engine.create_table('big_scan', [{'name': 'i', 'type': 'INT'}])
        tbl  = self.engine.open_table('big_scan')
        n    = 500
        rids = [tbl.insert({'i': k}) for k in range(n)]
        seen = {r[1]['i'] for r in tbl.scan()}
        assert seen == set(range(n))

    def test_all_null_record_roundtrip(self):
        self.engine.create_table('nulls', [
            {'name': 'a', 'type': 'INT'},
            {'name': 'b', 'type': 'TEXT'},
            {'name': 'c', 'type': 'FLOAT'},
        ])
        tbl = self.engine.open_table('nulls')
        rid = tbl.insert({'a': None, 'b': None, 'c': None})
        rec = tbl.get(rid)
        assert rec == {'a': None, 'b': None, 'c': None}

    def test_bool_type(self):
        self.engine.create_table('bools', [{'name': 'flag', 'type': 'BOOL'}])
        tbl  = self.engine.open_table('bools')
        r_t  = tbl.insert({'flag': True})
        r_f  = tbl.insert({'flag': False})
        assert tbl.get(r_t)['flag'] is True
        assert tbl.get(r_f)['flag'] is False

    def test_bigint_extremes(self):
        self.engine.create_table('bigs', [{'name': 'n', 'type': 'BIGINT'}])
        tbl     = self.engine.open_table('bigs')
        max_val = (2 ** 63) - 1
        min_val = -(2 ** 63)
        r1 = tbl.insert({'n': max_val})
        r2 = tbl.insert({'n': min_val})
        assert tbl.get(r1)['n'] == max_val
        assert tbl.get(r2)['n'] == min_val

    def test_scan_rid_matches_get(self):
        """Every RID yielded by scan must be resolvable via get."""
        self.engine.create_table('check', [{'name': 'x', 'type': 'INT'}])
        tbl = self.engine.open_table('check')
        for i in range(30):
            tbl.insert({'x': i})
        for rid, rec in tbl.scan():
            assert tbl.get(rid) == rec

    def test_update_to_null(self):
        self.engine.create_table('updnull', USERS_SCHEMA)
        tbl = self.engine.open_table('updnull')
        rid = tbl.insert(SAMPLE_USER)
        tbl.update(rid, {'id': 1, 'name': None, 'score': None})
        rec = tbl.get(rid)
        assert rec['name']  is None
        assert rec['score'] is None


# ---------------------------------------------------------------------------
# TestMultiTable
# ---------------------------------------------------------------------------

class TestMultiTable:

    def setup_method(self):
        self.tmpdir = tmp_dir()
        self.engine = StorageEngine(self.tmpdir)

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_two_tables_independent(self):
        self.engine.create_table('users',  USERS_SCHEMA)
        self.engine.create_table('orders', ORDERS_SCHEMA)
        u = self.engine.open_table('users')
        o = self.engine.open_table('orders')
        u.insert({'id': 1, 'name': 'Alice', 'score': 5.0})
        o.insert({'order_id': 100, 'item': 'Widget', 'qty': 3, 'shipped': False})
        assert u.count() == 1
        assert o.count() == 1

    def test_drop_one_leaves_other(self):
        self.engine.create_table('a', [{'name': 'v', 'type': 'INT'}])
        self.engine.create_table('b', [{'name': 'v', 'type': 'INT'}])
        self.engine.drop_table('a')
        assert not self.engine.table_exists('a')
        assert     self.engine.table_exists('b')

    def test_separate_heap_files(self):
        self.engine.create_table('alpha', [{'name': 'v', 'type': 'INT'}])
        self.engine.create_table('beta',  [{'name': 'v', 'type': 'INT'}])
        # Heap files are created lazily on first insert
        self.engine.open_table('alpha').insert({'v': 1})
        self.engine.open_table('beta').insert({'v': 2})
        assert os.path.exists(os.path.join(self.tmpdir, 'alpha.db'))
        assert os.path.exists(os.path.join(self.tmpdir, 'beta.db'))


# ---------------------------------------------------------------------------
# TestCompression
# ---------------------------------------------------------------------------

class TestCompression:

    def test_rle_encode_decode_int(self):
        vals = [1, 1, 1, 2, 2, 3, 3, 3, 3]
        rt   = RLEEncoder.decode(RLEEncoder.encode(vals, T_INT), T_INT)
        assert rt == vals

    def test_rle_encode_decode_bigint(self):
        vals = [10**15, 10**15, 0, 0, 0, -1]
        rt   = RLEEncoder.decode(RLEEncoder.encode(vals, T_BIGINT), T_BIGINT)
        assert rt == vals

    def test_rle_empty(self):
        assert RLEEncoder.decode(RLEEncoder.encode([], T_INT), T_INT) == []

    def test_rle_all_unique(self):
        vals = list(range(100))
        assert RLEEncoder.decode(RLEEncoder.encode(vals), T_INT) == vals

    def test_rle_all_same(self):
        vals = [7] * 1000
        rt   = RLEEncoder.decode(RLEEncoder.encode(vals), T_INT)
        assert rt == vals

    def test_rle_compression_ratio(self):
        vals  = [42] * 1000  # perfect run
        ratio = RLEEncoder.ratio(vals)
        assert ratio < 0.02  # 1000 ints = 4000 B → ~8 B compressed

    def test_dict_encode_decode(self):
        vals = ['apple', 'banana', 'apple', 'cherry', 'banana', 'apple']
        rt   = DictEncoder.decode(DictEncoder.encode(vals))
        assert rt == vals

    def test_dict_with_none(self):
        vals = ['a', None, 'b', None, 'a']
        rt   = DictEncoder.decode(DictEncoder.encode(vals))
        assert rt == ['a', '', 'b', '', 'a']

    def test_dict_empty(self):
        assert DictEncoder.decode(DictEncoder.encode([])) == []

    def test_dict_compression_ratio_low_cardinality(self):
        vals  = ['status_active'] * 500 + ['status_inactive'] * 500
        ratio = DictEncoder.ratio(vals)
        assert ratio < 0.3  # significant compression for repeated strings

    def test_scan_compressed_int(self):
        tmpdir = tmp_dir()
        try:
            engine = StorageEngine(tmpdir)
            engine.create_table('nums', [{'name': 'n', 'type': 'INT'}])
            tbl    = engine.open_table('nums')
            vals   = [1, 1, 1, 2, 2, 3]
            for v in vals:
                tbl.insert({'n': v})
            compressed = scan_compressed(tbl, 'n')
            decoded    = RLEEncoder.decode(compressed, T_INT)
            assert decoded == vals
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_scan_compressed_text(self):
        tmpdir = tmp_dir()
        try:
            engine = StorageEngine(tmpdir)
            engine.create_table('words', [{'name': 'w', 'type': 'TEXT'}])
            tbl    = engine.open_table('words')
            vals   = ['hello', 'world', 'hello']
            for v in vals:
                tbl.insert({'w': v})
            compressed = scan_compressed(tbl, 'w')
            decoded    = DictEncoder.decode(compressed)
            assert decoded == vals
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_scan_compressed_unknown_col(self):
        tmpdir = tmp_dir()
        try:
            engine = StorageEngine(tmpdir)
            engine.create_table('t', [{'name': 'x', 'type': 'INT'}])
            tbl    = engine.open_table('t')
            with pytest.raises(KeyError):
                scan_compressed(tbl, 'no_such_col')
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
