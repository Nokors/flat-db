"""
engine.py — Flat-DB Query Execution Engine  (Part 3)
======================================================

Architecture: Volcano / iterator model.

Every physical operator exposes:
    open()          — initialise state
    next() -> dict  — produce the next output row, or None when exhausted
    close()         — release resources
    __iter__()      — convenience wrapper: open → loop next → close

Pipeline for SELECT (built by build_plan):
    SeqScan
      └─ [HashJoin with right SeqScan]
           └─ [Filter]
                └─ [GroupByAggregate]
                     └─ [Sort]
                          └─ [Limit]
                               └─ Project

Scalar functions supported in expressions:
    UPPER(col)       — uppercase a text value
    LOWER(col)       — lowercase a text value
    SPLIT(col, sep)  — split on sep, return list joined with '|' for display

Aggregate functions (in GROUP BY queries):
    SUM, AVG, MIN, MAX
"""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Deferred imports from db_cli to avoid circular dependency at module load
# ---------------------------------------------------------------------------

def _ast():
    import db_cli as _m
    return _m


# =============================================================================
# 1. Expression evaluator
# =============================================================================

_AGG_FUNCS = {'SUM', 'AVG', 'MIN', 'MAX'}


def eval_expr(expr, row: dict) -> Any:
    """Evaluate an AST expression against a row dict.

    ColumnRef resolution order:
      1. Exact key match (handles qualified 'table.col' keys).
      2. Bare column name fallback (strips the 'table.' prefix from the key).
    """
    m = _ast()

    if isinstance(expr, m.Literal):
        return expr.value

    if isinstance(expr, m.ColumnRef):
        name = expr.name
        if name in row:
            return row[name]
        # unqualified fallback: look for any key ending with '.name'
        suffix = '.' + name
        for k, v in row.items():
            if k == name or k.endswith(suffix):
                return v
        return None

    if isinstance(expr, m.BinaryOp):
        op = expr.op
        # Short-circuit AND / OR before evaluating both sides
        if op == 'AND':
            lv = eval_expr(expr.left, row)
            return bool(lv) and bool(eval_expr(expr.right, row))
        if op == 'OR':
            lv = eval_expr(expr.left, row)
            return bool(lv) or bool(eval_expr(expr.right, row))
        lv = eval_expr(expr.left,  row)
        rv = eval_expr(expr.right, row)
        if lv is None or rv is None:
            return None
        if op == '=':  return lv == rv
        if op == '!=': return lv != rv
        if op == '<':  return lv <  rv
        if op == '>':  return lv >  rv
        if op == '<=': return lv <= rv
        if op == '>=': return lv >= rv
        if op == '+':
            try:
                return lv + rv
            except TypeError:
                return str(lv) + str(rv)
        if op == '-':  return lv - rv
        return None

    if isinstance(expr, m.FuncCall):
        fn = expr.func_name.upper()
        if fn in _AGG_FUNCS:
            # Aggregate has already been materialised by GroupByAggregate.
            # The row contains the result under the projection label key.
            label = _projection_label(expr)
            if label in row:
                return row[label]
            # Fallback: try resolving the aggregate column directly
            if expr.args:
                return _resolve_col(row, _projection_label(expr.args[0]))
            return None
        args = [eval_expr(a, row) for a in expr.args]
        return _apply_scalar(fn, args, row)

    return None


def _apply_scalar(fn: str, args: list, row: dict) -> Any:
    """Dispatch scalar function by name."""
    if fn == 'UPPER':
        v = args[0]
        return v.upper() if isinstance(v, str) else v
    if fn == 'LOWER':
        v = args[0]
        return v.lower() if isinstance(v, str) else v
    if fn == 'SPLIT':
        v   = args[0]
        sep = args[1] if len(args) > 1 else ' '
        if not isinstance(v, str):
            return v
        parts = v.split(str(sep))
        return '|'.join(parts)
    raise ValueError(f"Unknown scalar function: {fn!r}")


def _projection_label(expr) -> str:
    """Derive a display name for a projection expression."""
    m = _ast()
    if isinstance(expr, m.ColumnRef):
        # use the bare column name (drop qualifier for display)
        return expr.name.split('.')[-1]
    if isinstance(expr, m.FuncCall):
        inner = ', '.join(_projection_label(a) for a in expr.args)
        return f"{expr.func_name}({inner})"
    if isinstance(expr, m.Literal):
        return repr(expr.value)
    if isinstance(expr, m.BinaryOp):
        return f"({_projection_label(expr.left)}{expr.op}{_projection_label(expr.right)})"
    return str(expr)


# =============================================================================
# 2. Physical operators  (Volcano / iterator model)
# =============================================================================

class Operator(ABC):
    """Abstract base for all physical operators."""

    def open(self) -> None:
        """Initialise operator state."""

    @abstractmethod
    def next(self) -> Optional[dict]:
        """Return the next output row, or None when exhausted."""

    def close(self) -> None:
        """Release resources."""

    def __iter__(self) -> Iterator[dict]:
        self.open()
        try:
            while True:
                row = self.next()
                if row is None:
                    break
                yield row
        finally:
            self.close()


# ---------------------------------------------------------------------------
# SeqScan
# ---------------------------------------------------------------------------

class SeqScan(Operator):
    """Sequential table scan.  Yields every live row via TableHandle.scan()."""

    def __init__(self, table_handle, alias: Optional[str] = None):
        self._tbl   = table_handle
        self._alias = alias or table_handle.name
        self._iter  = None

    def open(self) -> None:
        self._iter = self._tbl.scan()

    def next(self) -> Optional[dict]:
        try:
            _rid, record = next(self._iter)
        except StopIteration:
            return None
        # Prefix every key with the alias so JOIN can distinguish columns
        return {f"{self._alias}.{k}": v for k, v in record.items()}

    def close(self) -> None:
        self._iter = None


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------

class Filter(Operator):
    """Evaluate a predicate on every row; pass through rows that satisfy it."""

    def __init__(self, child: Operator, predicate):
        self._child = child
        self._pred  = predicate

    def open(self) -> None:
        self._child.open()

    def next(self) -> Optional[dict]:
        while True:
            row = self._child.next()
            if row is None:
                return None
            if eval_expr(self._pred, row):
                return row

    def close(self) -> None:
        self._child.close()


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

class Project(Operator):
    """Apply projection expressions and rename output columns."""

    def __init__(self, child: Operator, projections, all_col_names: List[str]):
        """
        projections : '*' or list of AST Expr nodes
        all_col_names : ordered list of bare column names (for '*' expansion)
        """
        self._child    = child
        self._proj     = projections
        self._col_names = all_col_names
        self._labels: List[str] = []

    def open(self) -> None:
        self._child.open()
        m = _ast()
        if self._proj == '*':
            self._labels = list(self._col_names)
        else:
            self._labels = [_projection_label(e) for e in self._proj]

    @property
    def labels(self) -> List[str]:
        return self._labels

    def next(self) -> Optional[dict]:
        row = self._child.next()
        if row is None:
            return None
        m = _ast()
        if self._proj == '*':
            # Flatten qualified keys to bare names (last segment after '.')
            out: dict = {}
            for label in self._col_names:
                # Try exact match first, then suffix match
                if label in row:
                    out[label] = row[label]
                else:
                    suffix = '.' + label
                    for k, v in row.items():
                        if k == label or k.endswith(suffix):
                            out[label] = v
                            break
                    else:
                        out[label] = None
            return out
        out = {}
        for label, expr in zip(self._labels, self._proj):
            out[label] = eval_expr(expr, row)
        return out

    def close(self) -> None:
        self._child.close()


# ---------------------------------------------------------------------------
# Limit
# ---------------------------------------------------------------------------

class Limit(Operator):
    """Early-stop after *n* rows."""

    def __init__(self, child: Operator, n: int):
        self._child = child
        self._n     = n
        self._count = 0

    def open(self) -> None:
        self._child.open()
        self._count = 0

    def next(self) -> Optional[dict]:
        if self._count >= self._n:
            return None
        row = self._child.next()
        if row is None:
            return None
        self._count += 1
        return row

    def close(self) -> None:
        self._child.close()


# ---------------------------------------------------------------------------
# Sort
# ---------------------------------------------------------------------------

class Sort(Operator):
    """Materialise all rows then sort by one or more keys."""

    def __init__(self, child: Operator, order_by: list):
        """order_by: list of OrderByKey(col, ascending)"""
        self._child    = child
        self._order_by = order_by
        self._rows:  List[dict] = []
        self._index: int = 0

    def open(self) -> None:
        self._child.open()
        self._rows  = list(_drain(self._child))
        self._index = 0
        # Sort by keys in reverse order so primary key wins (stable sort)
        for key in reversed(self._order_by):
            self._rows.sort(
                key=lambda r: _sort_key(r, key.col),
                reverse=not key.ascending,
            )

    def next(self) -> Optional[dict]:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def close(self) -> None:
        self._child.close()
        self._rows = []


def _sort_key(row: dict, col: str):
    """Return a sort-safe key for the column value (None sorts last)."""
    val = row.get(col)
    if val is None:
        suffix = '.' + col
        for k, v in row.items():
            if k.endswith(suffix):
                val = v
                break
    if val is None:
        return (1, '')
    return (0, val)


# ---------------------------------------------------------------------------
# HashJoin
# ---------------------------------------------------------------------------

class HashJoin(Operator):
    """Hash join (equi-join only).

    Builds a hash table on the *right* side, then probes with each *left* row.
    """

    def __init__(
        self,
        left:       Operator,
        right:      Operator,
        left_key:   str,
        right_key:  str,
    ):
        self._left      = left
        self._right     = right
        self._left_key  = left_key
        self._right_key = right_key
        self._table: Dict[Any, List[dict]] = defaultdict(list)
        self._pending: List[dict] = []
        self._left_iter = None

    def open(self) -> None:
        # Build phase: materialise right side into hash table
        self._right.open()
        self._table = defaultdict(list)
        while True:
            row = self._right.next()
            if row is None:
                break
            key = _resolve_col(row, self._right_key)
            self._table[key].append(row)
        self._right.close()

        self._left.open()
        self._pending = []

    def next(self) -> Optional[dict]:
        while True:
            if self._pending:
                return self._pending.pop(0)
            left_row = self._left.next()
            if left_row is None:
                return None
            key = _resolve_col(left_row, self._left_key)
            matches = self._table.get(key, [])
            for right_row in matches:
                merged = {**left_row, **right_row}
                self._pending.append(merged)
            # If no matches, continue to next left row (inner join semantics)

    def close(self) -> None:
        self._left.close()
        self._table = {}
        self._pending = []


def _resolve_col(row: dict, col: str) -> Any:
    """Look up *col* in *row*, accepting bare or qualified names."""
    if col in row:
        return row[col]
    suffix = '.' + col
    for k, v in row.items():
        if k == col or k.endswith(suffix):
            return v
    return None


# ---------------------------------------------------------------------------
# GroupByAggregate
# ---------------------------------------------------------------------------

class GroupByAggregate(Operator):
    """Materialise + group rows, then apply one aggregate function.

    Supports GROUP BY with one aggregate (SUM / AVG / MIN / MAX) in the
    SELECT list.  Rows without GROUP BY are treated as a single group.
    """

    def __init__(
        self,
        child:       Operator,
        group_by:    List[str],       # bare or qualified column names
        agg_func:    str,             # 'SUM' | 'AVG' | 'MIN' | 'MAX'
        agg_col:     str,             # column to aggregate
        agg_label:   str,             # output column name, e.g. "MAX(score)"
    ):
        self._child     = child
        self._group_by  = group_by
        self._agg_func  = agg_func.upper()
        self._agg_col   = agg_col
        self._agg_label = agg_label
        self._results:  List[dict] = []
        self._index:    int = 0

    def open(self) -> None:
        self._child.open()
        rows = list(_drain(self._child))
        self._results = self._aggregate(rows)
        self._index = 0

    def _aggregate(self, rows: List[dict]) -> List[dict]:
        # group_key → list of agg values
        buckets: Dict[tuple, list] = defaultdict(list)
        group_vals: Dict[tuple, dict] = {}

        for row in rows:
            key = tuple(_resolve_col(row, c) for c in self._group_by)
            val = _resolve_col(row, self._agg_col)
            if val is not None:
                buckets[key].append(val)
            elif key not in buckets:
                buckets[key] = []
            # keep one sample of group-by column values
            if key not in group_vals:
                group_vals[key] = {
                    c: _resolve_col(row, c) for c in self._group_by
                }

        results = []
        fn = self._agg_func
        for key, vals in buckets.items():
            out = dict(group_vals.get(key, {}))
            if not vals:
                agg_result = None
            elif fn == 'SUM':
                agg_result = sum(vals)
            elif fn == 'AVG':
                agg_result = sum(vals) / len(vals)
            elif fn == 'MIN':
                agg_result = min(vals)
            elif fn == 'MAX':
                agg_result = max(vals)
            else:
                raise ValueError(f"Unsupported aggregate: {fn}")
            out[self._agg_label] = agg_result
            results.append(out)
        return results

    def next(self) -> Optional[dict]:
        if self._index >= len(self._results):
            return None
        row = self._results[self._index]
        self._index += 1
        return row

    def close(self) -> None:
        self._child.close()
        self._results = []


# ---------------------------------------------------------------------------
# InsertOp
# ---------------------------------------------------------------------------

class InsertOp(Operator):
    """Insert a single row into a table; yields one summary row."""

    def __init__(self, table_handle, schema, values_exprs: list):
        self._tbl    = table_handle
        self._schema = schema
        self._values = values_exprs
        self._done   = False

    def open(self) -> None:
        self._done = False

    def next(self) -> Optional[dict]:
        if self._done:
            return None
        self._done = True
        record = _build_record(self._schema, self._values)
        rid = self._tbl.insert(record)
        return {'result': f'Inserted 1 row. RID={rid}'}

    def close(self) -> None:
        pass


def _build_record(schema, values_exprs) -> dict:
    """Coerce parsed literal values to column types and return a record dict."""
    import db_cli as m
    record: dict = {}
    for col, val_expr in zip(schema, values_exprs):
        raw = val_expr.value if isinstance(val_expr, m.Literal) else eval_expr(val_expr, {})
        if raw is None:
            record[col.name] = None
        elif col.col_type == 'INT':
            record[col.name] = int(raw)
        elif col.col_type == 'BIGINT':
            record[col.name] = int(raw)
        elif col.col_type == 'FLOAT':
            record[col.name] = float(raw)
        elif col.col_type == 'BOOL':
            record[col.name] = bool(raw)
        else:
            record[col.name] = str(raw)
    return record


# ---------------------------------------------------------------------------
# DeleteOp
# ---------------------------------------------------------------------------

class DeleteOp(Operator):
    """Scan a table, apply a predicate, delete matching rows."""

    def __init__(self, table_handle, predicate, alias: str):
        self._tbl  = table_handle
        self._pred = predicate
        self._alias = alias
        self._done = False
        self._count = 0

    def open(self) -> None:
        self._done  = False
        self._count = 0

    def next(self) -> Optional[dict]:
        if self._done:
            return None
        self._done = True
        rids_to_delete = []
        for rid, record in self._tbl.scan():
            qualified = {f"{self._alias}.{k}": v for k, v in record.items()}
            if self._pred is None or eval_expr(self._pred, qualified):
                rids_to_delete.append(rid)
        for rid in rids_to_delete:
            self._tbl.delete(rid)
        self._count = len(rids_to_delete)
        return {'result': f'Deleted {self._count} row{"s" if self._count != 1 else ""}'}

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# CTASop  (Create Table As Select)
# ---------------------------------------------------------------------------

class CTASop(Operator):
    """Execute a SELECT plan, infer schema from results, create a new table."""

    def __init__(self, new_table: str, select_plan: Operator, engine, select_stmt):
        self._new_table = new_table
        self._plan      = select_plan
        self._engine    = engine
        self._stmt      = select_stmt
        self._done      = False

    def open(self) -> None:
        self._done = False

    def next(self) -> Optional[dict]:
        if self._done:
            return None
        self._done = True

        rows = list(self._plan)
        if not rows:
            # Create an empty table using schema inferred from projection labels
            labels = _infer_labels(self._stmt, self._engine)
            schema = [{'name': lbl, 'type': 'TEXT'} for lbl in labels]
            self._engine.create_table(self._new_table, schema)
            return {'result': f"Table '{self._new_table}' created (0 rows)."}

        # Infer schema from the first row
        schema = []
        sample = rows[0]
        for col_name, val in sample.items():
            col_type = _infer_type(val)
            schema.append({'name': col_name, 'type': col_type})
        self._engine.create_table(self._new_table, schema)
        tbl = self._engine.open_table(self._new_table)
        for row in rows:
            tbl.insert(row)
        n = len(rows)
        return {'result': f"Table '{self._new_table}' created ({n} row{'s' if n != 1 else ''})."}

    def close(self) -> None:
        pass


def _infer_type(val: Any) -> str:
    if isinstance(val, bool):
        return 'BOOL'
    if isinstance(val, int):
        return 'INT'
    if isinstance(val, float):
        return 'FLOAT'
    return 'TEXT'


def _infer_labels(stmt, engine) -> List[str]:
    """Return column labels from a SelectStmt (best-effort, no execution)."""
    import db_cli as m
    if stmt.projections == '*':
        if engine.table_exists(stmt.from_table):
            tbl = engine.open_table(stmt.from_table)
            return [c.name for c in tbl.schema]
        return []
    return [_projection_label(e) for e in stmt.projections]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _drain(op: Operator):
    """Fully materialise an already-opened operator."""
    while True:
        row = op.next()
        if row is None:
            break
        yield row


# =============================================================================
# 3. Planner  —  AST → operator tree
# =============================================================================

def build_plan(stmt, engine) -> Operator:
    """Translate a parsed AST statement into a physical operator tree."""
    import db_cli as m

    if isinstance(stmt, m.InsertStmt):
        return _plan_insert(stmt, engine)

    if isinstance(stmt, m.DeleteStmt):
        return _plan_delete(stmt, engine)

    if isinstance(stmt, m.CreateTableAsStmt):
        select_plan = build_plan(stmt.select, engine)
        return CTASop(stmt.table_name, select_plan, engine, stmt.select)

    if isinstance(stmt, m.SelectStmt):
        return _plan_select(stmt, engine)

    raise ValueError(f"Cannot build execution plan for {type(stmt).__name__}")


# ------------------------------------------------------------------ INSERT

def _plan_insert(stmt, engine) -> InsertOp:
    import db_cli as m
    if not engine.table_exists(stmt.table_name):
        _err(f"Table '{stmt.table_name}' does not exist")
    tbl    = engine.open_table(stmt.table_name)
    schema = tbl.schema
    if len(stmt.values) != len(schema):
        _err(f"INSERT expects {len(schema)} values, got {len(stmt.values)}")
    return InsertOp(tbl, schema, stmt.values)


# ------------------------------------------------------------------ DELETE

def _plan_delete(stmt, engine) -> DeleteOp:
    if not engine.table_exists(stmt.table_name):
        _err(f"Table '{stmt.table_name}' does not exist")
    tbl   = engine.open_table(stmt.table_name)
    alias = stmt.table_name
    return DeleteOp(tbl, stmt.selection, alias)


# ------------------------------------------------------------------ SELECT

def _plan_select(stmt, engine) -> Operator:
    import db_cli as m

    left_alias  = stmt.from_alias or stmt.from_table

    if not engine.table_exists(stmt.from_table):
        _err(f"Table '{stmt.from_table}' does not exist")
    left_tbl = engine.open_table(stmt.from_table)
    node: Operator = SeqScan(left_tbl, alias=left_alias)

    # --- JOIN ---
    if stmt.join is not None:
        jc = stmt.join
        if not engine.table_exists(jc.right_table):
            _err(f"Table '{jc.right_table}' does not exist")
        right_alias = jc.right_alias or jc.right_table
        right_tbl   = engine.open_table(jc.right_table)
        right_scan  = SeqScan(right_tbl, alias=right_alias)

        # Extract equi-join key columns from the ON condition
        left_key, right_key = _extract_join_keys(jc.condition, left_alias, right_alias)
        node = HashJoin(node, right_scan, left_key=left_key, right_key=right_key)

    # --- WHERE ---
    if stmt.selection is not None:
        node = Filter(node, stmt.selection)

    # --- GROUP BY / aggregate ---
    agg_info = _find_aggregate(stmt.projections)
    if stmt.group_by or agg_info:
        group_by   = stmt.group_by or []
        func, col, label = agg_info if agg_info else ('MAX', group_by[0] if group_by else '', '')
        node = GroupByAggregate(
            child=node,
            group_by=group_by,
            agg_func=func,
            agg_col=col,
            agg_label=label,
        )

    # --- ORDER BY ---
    if stmt.order_by:
        node = Sort(node, stmt.order_by)

    # --- LIMIT ---
    if stmt.limit is not None:
        node = Limit(node, stmt.limit)

    # --- PROJECT ---
    all_col_names = _all_output_cols(stmt, engine)
    node = Project(node, stmt.projections, all_col_names)
    return node


def _extract_join_keys(condition, left_alias: str, right_alias: str) -> Tuple[str, str]:
    """Pull the two column names out of an equi-join ON condition."""
    import db_cli as m
    if isinstance(condition, m.BinaryOp) and condition.op == '=':
        left_col  = condition.left.name  if isinstance(condition.left,  m.ColumnRef) else ''
        right_col = condition.right.name if isinstance(condition.right, m.ColumnRef) else ''
        # Determine which side belongs to left/right table
        def _belongs_left(col_name: str) -> bool:
            if '.' in col_name:
                prefix = col_name.split('.')[0]
                return prefix == left_alias
            return True  # ambiguous → assume left

        if _belongs_left(left_col):
            return left_col, right_col
        else:
            return right_col, left_col
    _err("JOIN ON clause must be a simple equi-join: a.col = b.col")


def _find_aggregate(projections):
    """Return (func_name, col_name, label) if any projection is an aggregate FuncCall."""
    import db_cli as m
    if projections == '*':
        return None
    for expr in projections:
        if isinstance(expr, m.FuncCall) and expr.func_name.upper() in {'SUM', 'AVG', 'MIN', 'MAX'}:
            label    = _projection_label(expr)
            agg_col  = _projection_label(expr.args[0]) if expr.args else ''
            return (expr.func_name.upper(), agg_col, label)
    return None


def _all_output_cols(stmt, engine) -> List[str]:
    """Return the ordered list of bare column names for '*' projection expansion."""
    cols = []
    left_alias = stmt.from_alias or stmt.from_table
    if engine.table_exists(stmt.from_table):
        for c in engine.open_table(stmt.from_table).schema:
            cols.append(c.name)
    if stmt.join and engine.table_exists(stmt.join.right_table):
        right_alias = stmt.join.right_alias or stmt.join.right_table
        for c in engine.open_table(stmt.join.right_table).schema:
            cols.append(c.name)
    return cols


# =============================================================================
# 4. Output formatter
# =============================================================================

def run_plan(plan: Operator, stmt, engine, out=None) -> None:
    """Iterate *plan*, print tabular output to *out*, handle messaging."""
    import db_cli as m
    if out is None:
        out = sys.stdout

    # Special single-message operators (INSERT, DELETE, CTAS)
    if isinstance(plan, (InsertOp, DeleteOp, CTASop)):
        for row in plan:
            print(row.get('result', ''), file=out)
        return

    # For SELECT-based plans, collect all rows and determine column labels
    plan.open()
    try:
        rows = list(_drain(plan))
    finally:
        plan.close()

    # Determine column header labels
    if isinstance(plan, Project):
        labels = plan.labels
    elif rows:
        labels = list(rows[0].keys())
    else:
        labels = _infer_labels(stmt, engine) if isinstance(stmt, m.SelectStmt) else []

    # Print header
    col_w = max(14, *(len(l) + 2 for l in labels)) if labels else 14
    print('  '.join(f'{l:<{col_w}}' for l in labels), file=out)
    print('  '.join('-' * col_w for _ in labels), file=out)

    # Print rows
    for row in rows:
        values = [str(row.get(l, '')) for l in labels]
        print('  '.join(f'{v:<{col_w}}' for v in values), file=out)

    n = len(rows)
    print(f'({n} row{"s" if n != 1 else ""})', file=out)


# =============================================================================
# 5. Utilities
# =============================================================================

def _err(msg: str) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)
