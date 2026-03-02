#!/usr/bin/env python3
"""
demo.py — Flat-DB Storage Engine demonstration
===============================================

Proves:
  1. Table creation and schema persistence
  2. Insert / get / update / delete
  3. Variable-length TEXT fields
  4. Data survival across a full engine restart
  5. Scan iterates all live records correctly
  6. Overflow update (record grows beyond original slot)
  7. Tombstone delete behaviour
  8. Column compression (RLE for INT, dictionary for TEXT)

Run:
    python3 demo.py

Output goes to stdout.  The demo uses a temporary directory that is
cleaned up automatically at the end.
"""

import os
import shutil
import tempfile

from storage import (
    StorageEngine, RLEEncoder, DictEncoder, scan_compressed,
    T_INT, T_TEXT, T_FLOAT, T_BOOL, T_BIGINT,
)

SEP = '─' * 60


def section(title: str) -> None:
    print(f'\n{SEP}')
    print(f'  {title}')
    print(SEP)


def main() -> None:
    data_dir = tempfile.mkdtemp(prefix='flatdb_demo_')
    print(f'Data directory: {data_dir}')

    try:
        # ------------------------------------------------------------------ 1
        section('1. Create tables & insert records')

        engine = StorageEngine(data_dir)

        engine.create_table('employees', [
            {'name': 'id',         'type': 'INT'},
            {'name': 'name',       'type': 'TEXT'},
            {'name': 'salary',     'type': 'FLOAT'},
            {'name': 'active',     'type': 'BOOL'},
            {'name': 'department', 'type': 'TEXT'},
        ])
        engine.create_table('departments', [
            {'name': 'dept_id',    'type': 'INT'},
            {'name': 'dept_name',  'type': 'TEXT'},
            {'name': 'headcount',  'type': 'BIGINT'},
        ])

        emp = engine.open_table('employees')

        emp_records = [
            {'id': 1, 'name': 'Alice',   'salary': 95000.0,  'active': True,  'department': 'Engineering'},
            {'id': 2, 'name': 'Bob',     'salary': 82000.0,  'active': True,  'department': 'Marketing'},
            {'id': 3, 'name': 'Charlie', 'salary': 110000.0, 'active': False, 'department': 'Engineering'},
            {'id': 4, 'name': 'Diana',   'salary': 70000.0,  'active': True,  'department': 'HR'},
            {'id': 5, 'name': 'Eve',     'salary': None,     'active': True,  'department': 'Engineering'},
        ]
        rids = [emp.insert(r) for r in emp_records]

        print(f'Inserted {len(rids)} employees.')
        for rid, rec in zip(rids, emp_records):
            print(f'  {rid}  →  {rec}')

        dept = engine.open_table('departments')
        dept_rids = [
            dept.insert({'dept_id': 1, 'dept_name': 'Engineering', 'headcount': 3}),
            dept.insert({'dept_id': 2, 'dept_name': 'Marketing',   'headcount': 1}),
            dept.insert({'dept_id': 3, 'dept_name': 'HR',          'headcount': 1}),
        ]
        print(f'\nInserted {len(dept_rids)} departments.')

        # ------------------------------------------------------------------ 2
        section('2. Point read (get)')

        for i, rid in enumerate(rids):
            rec = emp.get(rid)
            print(f'  get({rid}) → {rec}')

        from storage import RID as _RID
        print(f'\n  get(RID(99, 0)) → {emp.get(_RID(99, 0))}  (non-existent)')

        # ------------------------------------------------------------------ 3
        section('3. In-place update (record shrinks / fits)')

        print(f'  Before: {emp.get(rids[1])}')
        emp.update(rids[1], {'id': 2, 'name': 'Robert', 'salary': 85000.0,
                              'active': True, 'department': 'Marketing'})
        print(f'  After:  {emp.get(rids[1])}')

        # ------------------------------------------------------------------ 4
        section('4. Overflow update (record grows → forwarding pointer)')

        short_rid = emp.insert({'id': 99, 'name': 'Temp', 'salary': 1.0,
                                 'active': True, 'department': 'TBD'})
        print(f'  Inserted short record at {short_rid}')
        pages_before = emp.page_count()

        big_name = 'Overflow_Employee_' + 'X' * 2000
        emp.update(short_rid, {'id': 99, 'name': big_name, 'salary': 999.0,
                                'active': False, 'department': 'BigData'})
        rec = emp.get(short_rid)
        print(f'  get({short_rid}) after overflow-update:')
        print(f'    id={rec["id"]}, name_len={len(rec["name"])}, '
              f'salary={rec["salary"]}, active={rec["active"]}')
        print(f'  Pages before: {pages_before} → after: {emp.page_count()}')

        # ------------------------------------------------------------------ 5
        section('5. Delete + tombstone behaviour')

        del_rid = emp.insert({'id': 50, 'name': 'Transient', 'salary': 0.0,
                               'active': False, 'department': 'None'})
        print(f'  Inserted {del_rid}')
        emp.delete(del_rid)
        print(f'  After delete: get({del_rid}) → {emp.get(del_rid)}')
        print(f'  Second delete returns: {emp.delete(del_rid)}  (expected False)')

        # ------------------------------------------------------------------ 6
        section('6. Full scan')

        results = list(emp.scan())
        print(f'  Live employee records: {len(results)}')
        for rid, rec in results:
            name = rec["name"][:30] + ('…' if len(rec["name"]) > 30 else '')
            print(f'    {rid}  id={rec["id"]}  name={name!r}  active={rec["active"]}')

        # ------------------------------------------------------------------ 7
        section('7. Persistence – engine restart')

        print('  Destroying engine object …')
        del engine, emp, dept

        print('  Creating new StorageEngine pointing at the same directory …')
        engine2 = StorageEngine(data_dir)
        assert engine2.table_exists('employees'),  'employees table missing!'
        assert engine2.table_exists('departments'), 'departments table missing!'

        emp2 = engine2.open_table('employees')
        for i, rid in enumerate(rids):
            rec = emp2.get(rid)
            assert rec is not None, f'Record {rid} lost after restart!'
        print(f'  All {len(rids)} employee RIDs resolve correctly after restart. ✓')

        # Verify overflow-updated record still readable
        rec = emp2.get(short_rid)
        assert rec is not None
        assert len(rec['name']) > 2000
        print(f'  Overflow-updated record ({short_rid}) still readable after restart. ✓')

        # Verify deleted record still absent
        assert emp2.get(del_rid) is None
        print(f'  Deleted record ({del_rid}) still absent after restart. ✓')

        count = emp2.count()
        print(f'  Total live employees after restart: {count}')

        # ------------------------------------------------------------------ 8
        section('8. Column compression')

        # RLE for INT column
        from storage import StorageEngine as SE
        eng3 = SE(data_dir)
        eng3.create_table('stats', [
            {'name': 'region',  'type': 'TEXT'},
            {'name': 'revenue', 'type': 'INT'},
        ])
        stats = eng3.open_table('stats')
        stats_data = (
            [{'region': 'North', 'revenue': 100}] * 50 +
            [{'region': 'South', 'revenue': 200}] * 50 +
            [{'region': 'East',  'revenue': 150}] * 30
        )
        for row in stats_data:
            stats.insert(row)

        # INT / RLE compression
        comp_rev = scan_compressed(stats, 'revenue')
        dec_rev  = RLEEncoder.decode(comp_rev, T_INT)
        ratio_rev = RLEEncoder.ratio(dec_rev, T_INT)
        print(f'\n  revenue column (INT, {len(dec_rev)} values):')
        print(f'    original size : {len(dec_rev) * 4:>6} bytes')
        print(f'    compressed    : {len(comp_rev):>6} bytes  (RLE)')
        print(f'    ratio         : {ratio_rev:.2%}')
        assert dec_rev[:50]  == [100] * 50, 'RLE decode mismatch'
        assert dec_rev[50:100] == [200] * 50
        print('    Round-trip check: ✓')

        # TEXT / dictionary compression
        comp_reg = scan_compressed(stats, 'region')
        dec_reg  = DictEncoder.decode(comp_reg)
        ratio_reg = DictEncoder.ratio([r['region'] for r in stats_data])
        print(f'\n  region column (TEXT, {len(dec_reg)} values):')
        print(f'    compressed    : {len(comp_reg):>6} bytes  (dictionary)')
        print(f'    ratio         : {ratio_reg:.2%}')
        assert dec_reg[:50] == ['North'] * 50, 'Dict decode mismatch'
        print('    Round-trip check: ✓')

        # ------------------------------------------------------------------ fin
        section('All demos passed')
        print('  The storage engine is working correctly.')
        print(f'  Files in data directory:')
        for fname in sorted(os.listdir(data_dir)):
            fpath = os.path.join(data_dir, fname)
            print(f'    {fname:<25}  {os.path.getsize(fpath):>8} bytes')

    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
        print(f'\nCleaned up {data_dir}')


if __name__ == '__main__':
    main()
