import sys
import re
import json
import argparse
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Union, Any

# =============================================================================
# 1. AST NODES
# =============================================================================

@dataclass
class Expr:
    pass

@dataclass
class ColumnRef(Expr):
    name: str           # bare "col" or qualified "table.col"
    type: str = "ColumnRef"

@dataclass
class Literal(Expr):
    value: Any
    type: str = "Literal"

@dataclass
class BinaryOp(Expr):
    left: Expr
    op: str
    right: Expr
    type: str = "BinaryOp"

@dataclass
class FuncCall(Expr):
    """Covers both scalar functions (UPPER, LOWER, SPLIT) and aggregates (MAX, MIN, SUM, AVG)."""
    func_name: str
    args: List[Expr]
    type: str = "FuncCall"

@dataclass
class OrderByKey:
    col: str            # bare or qualified column name
    ascending: bool = True

@dataclass
class JoinClause:
    right_table: str
    right_alias: Optional[str]
    condition: Expr     # typically BinaryOp(ColumnRef, '=', ColumnRef)
    join_type: str = "HASH"

@dataclass
class ColumnDef:
    name: str
    data_type: str

@dataclass
class Statement:
    pass

@dataclass
class CreateTableStmt(Statement):
    table_name: str
    columns: List[ColumnDef]
    type: str = "CREATE_TABLE"

@dataclass
class CreateTableAsStmt(Statement):
    table_name: str
    select: 'SelectStmt'
    type: str = "CTAS"

@dataclass
class InsertStmt(Statement):
    table_name: str
    values: List[Expr]
    type: str = "INSERT"

@dataclass
class SelectStmt(Statement):
    projections: Union[str, List[Expr]]   # '*' or list of Expr
    from_table: str
    from_alias: Optional[str] = None
    join: Optional[JoinClause] = None
    selection: Optional[Expr] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[OrderByKey]] = None
    limit: Optional[int] = None
    type: str = "SELECT"

@dataclass
class DeleteStmt(Statement):
    table_name: str
    selection: Optional[Expr] = None
    type: str = "DELETE"

@dataclass
class _DropTableStmt(Statement):
    table_name: str
    type: str = "DROP_TABLE"


# =============================================================================
# 2. LEXER
# =============================================================================

# Aggregate and scalar function names treated as keywords so they don't
# collide with table/column identifiers that share the same spelling.
_AGG_FUNCS    = {'SUM', 'AVG', 'MIN', 'MAX'}
_SCALAR_FUNCS = {'UPPER', 'LOWER', 'SPLIT'}
_FUNC_NAMES   = _AGG_FUNCS | _SCALAR_FUNCS

TOKEN_SPEC = [
    ('NUMBER',   r'-?\d+(\.\d+)?'),
    ('STRING',   r"'(?:''|[^'])*'"),
    ('KEYWORD',  r'\b(SELECT|FROM|WHERE|CREATE|TABLE|INSERT|INTO|VALUES|LIMIT|AND|OR'
                 r'|INT|BIGINT|FLOAT|TEXT|BOOL|NULL|DROP|SCAN'
                 r'|ORDER|BY|ASC|DESC|JOIN|HASH|ON|GROUP|DELETE|AS'
                 r'|SUM|AVG|MIN|MAX|UPPER|LOWER|SPLIT)\b'),
    ('ID',       r'[a-zA-Z_][a-zA-Z0-9_]*'),
    ('OP',       r'!=|<=|>=|[=<>*]'),
    ('DELIM',    r'[(),;.]'),
    ('SKIP',     r'[ \t\n\r]+'),
    ('MISMATCH', r'.'),
]

@dataclass
class Token:
    type: str
    value: str
    pos: int

class Lexer:
    def __init__(self, code):
        self.tokens = []
        self.tokenize(code)

    def tokenize(self, code):
        regex = '|'.join('(?P<%s>%s)' % pair for pair in TOKEN_SPEC)
        for mo in re.finditer(regex, code, re.IGNORECASE):
            kind  = mo.lastgroup
            value = mo.group()
            pos   = mo.start()
            if kind == 'SKIP':
                continue
            elif kind == 'MISMATCH':
                raise ValueError(f"Unexpected character '{value}' at position {pos}")
            else:
                if kind == 'KEYWORD':
                    value = value.upper()
                self.tokens.append(Token(kind, value, pos))
        self.tokens.append(Token('EOF', '', len(code)))


# =============================================================================
# 3. PARSER
# =============================================================================

_SUPPORTED_TYPES = {'INT', 'BIGINT', 'FLOAT', 'TEXT', 'BOOL'}

class Parser:
    def __init__(self, tokens, raw_query):
        self.tokens    = tokens
        self.raw_query = raw_query
        self.pos       = 0

    def peek(self):
        return self.tokens[self.pos]

    def peek2(self):
        """Look two tokens ahead (for HASH JOIN detection)."""
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return Token('EOF', '', -1)

    def consume(self, expected_type=None, expected_value=None):
        token = self.peek()
        if expected_type and token.type != expected_type:
            self.error(f"Expected {expected_type}, got {token.type} ({token.value!r})")
        if expected_value and token.value.upper() != expected_value.upper():
            self.error(f"Expected '{expected_value}', got '{token.value}'")
        self.pos += 1
        return token

    def error(self, message):
        token        = self.peek()
        line_preview = self.raw_query[max(0, token.pos - 10): token.pos + 10]
        full_msg     = (f"Syntax Error at position {token.pos}: {message}\n"
                        f"Near: ...{line_preview}...")
        print(full_msg, file=sys.stderr)
        sys.exit(1)

    def parse(self):
        tok = self.peek()
        if tok.value == 'CREATE': return self.parse_create()
        if tok.value == 'DROP':   return self.parse_drop()
        if tok.value == 'INSERT': return self.parse_insert()
        if tok.value == 'SELECT': return self.parse_select()
        if tok.value == 'SCAN':   return self.parse_scan()
        if tok.value == 'DELETE': return self.parse_delete()
        self.error(f"Unsupported statement start: {tok.value!r}")

    # ------------------------------------------------------------------ DDL

    def parse_create(self):
        self.consume('KEYWORD', 'CREATE')
        self.consume('KEYWORD', 'TABLE')
        table_name = self.consume('ID').value
        # CTAS: CREATE TABLE x AS SELECT ...
        if self.peek().value == 'AS':
            self.consume('KEYWORD', 'AS')
            select_stmt = self.parse_select()
            return CreateTableAsStmt(table_name=table_name, select=select_stmt)
        # Regular CREATE TABLE
        self.consume('DELIM', '(')
        columns = []
        while True:
            col_name = self.consume('ID').value
            col_type = self.consume('KEYWORD').value.upper()
            if col_type not in _SUPPORTED_TYPES:
                self.error(f"Unsupported data type {col_type!r}")
            columns.append(ColumnDef(col_name, col_type))
            if self.peek().value == ')':
                break
            self.consume('DELIM', ',')
        self.consume('DELIM', ')')
        return CreateTableStmt(table_name=table_name, columns=columns)

    def parse_drop(self):
        self.consume('KEYWORD', 'DROP')
        self.consume('KEYWORD', 'TABLE')
        table_name = self.consume('ID').value
        return _DropTableStmt(table_name=table_name)

    # ------------------------------------------------------------------ DML

    def parse_insert(self):
        self.consume('KEYWORD', 'INSERT')
        self.consume('KEYWORD', 'INTO')
        table_name = self.consume('ID').value
        self.consume('KEYWORD', 'VALUES')
        self.consume('DELIM', '(')
        values = []
        while True:
            values.append(self.parse_expression())
            if self.peek().value == ')':
                break
            self.consume('DELIM', ',')
        self.consume('DELIM', ')')
        return InsertStmt(table_name=table_name, values=values)

    def parse_delete(self):
        self.consume('KEYWORD', 'DELETE')
        self.consume('KEYWORD', 'FROM')
        table_name = self.consume('ID').value
        where_clause = None
        if self.peek().value == 'WHERE':
            self.consume('KEYWORD', 'WHERE')
            where_clause = self.parse_expression()
        return DeleteStmt(table_name=table_name, selection=where_clause)

    def parse_select(self):
        self.consume('KEYWORD', 'SELECT')

        # --- projections ---
        projections: Union[str, List[Expr]]
        if self.peek().value == '*':
            self.consume('OP', '*')
            projections = '*'
        else:
            projections = []
            while True:
                projections.append(self.parse_projection_expr())
                if self.peek().value != ',':
                    break
                self.consume('DELIM', ',')
            if not projections:
                self.error("SELECT requires at least one column or '*'")

        # --- FROM table [alias] ---
        self.consume('KEYWORD', 'FROM')
        table_name = self.consume('ID').value
        from_alias = None
        if self.peek().type == 'ID' and self.peek().value.upper() not in {
            'WHERE', 'JOIN', 'HASH', 'ON', 'GROUP', 'ORDER', 'LIMIT', 'AND', 'OR',
            'ASC', 'DESC', 'BY', 'INNER'
        }:
            from_alias = self.consume('ID').value

        # --- [HASH] JOIN table [alias] ON cond ---
        join_clause = None
        if self.peek().value in ('JOIN', 'HASH', 'INNER'):
            join_type = "HASH"
            if self.peek().value == 'HASH':
                self.consume('KEYWORD', 'HASH')
                join_type = "HASH"
            elif self.peek().value == 'INNER':
                self.consume('KEYWORD', 'INNER')
                join_type = "INNER"
            self.consume('KEYWORD', 'JOIN')
            right_table = self.consume('ID').value
            right_alias = None
            if self.peek().type == 'ID' and self.peek().value.upper() not in {
                'ON', 'WHERE', 'GROUP', 'ORDER', 'LIMIT'
            }:
                right_alias = self.consume('ID').value
            self.consume('KEYWORD', 'ON')
            join_cond = self.parse_expression()
            join_clause = JoinClause(
                right_table=right_table,
                right_alias=right_alias,
                condition=join_cond,
                join_type=join_type,
            )

        # --- WHERE ---
        where_clause = None
        if self.peek().value == 'WHERE':
            self.consume('KEYWORD', 'WHERE')
            where_clause = self.parse_expression()

        # --- GROUP BY col [, col] ---
        group_by = None
        if self.peek().value == 'GROUP':
            self.consume('KEYWORD', 'GROUP')
            self.consume('KEYWORD', 'BY')
            group_by = []
            while True:
                group_by.append(self._parse_qualified_name())
                if self.peek().value != ',':
                    break
                self.consume('DELIM', ',')

        # --- ORDER BY col [ASC|DESC] [, col [ASC|DESC]] ---
        order_by = None
        if self.peek().value == 'ORDER':
            self.consume('KEYWORD', 'ORDER')
            self.consume('KEYWORD', 'BY')
            order_by = []
            while True:
                col_name = self._parse_qualified_name()
                ascending = True
                if self.peek().value == 'DESC':
                    self.consume('KEYWORD', 'DESC')
                    ascending = False
                elif self.peek().value == 'ASC':
                    self.consume('KEYWORD', 'ASC')
                order_by.append(OrderByKey(col=col_name, ascending=ascending))
                if self.peek().value != ',':
                    break
                self.consume('DELIM', ',')

        # --- LIMIT n ---
        limit_val = None
        if self.peek().value == 'LIMIT':
            self.consume('KEYWORD', 'LIMIT')
            limit_val = int(self.consume('NUMBER').value)

        return SelectStmt(
            projections=projections,
            from_table=table_name,
            from_alias=from_alias,
            join=join_clause,
            selection=where_clause,
            group_by=group_by,
            order_by=order_by,
            limit=limit_val,
        )

    def parse_scan(self):
        """SCAN <table>  — full table scan shorthand."""
        self.consume('KEYWORD', 'SCAN')
        table_name = self.consume('ID').value
        return SelectStmt('*', table_name)

    # ------------------------------------------------------------------ expressions

    def _parse_qualified_name(self) -> str:
        """Parse `name` or `table.col` and return as a string."""
        name = self.consume('ID').value
        if self.peek().value == '.' and self.peek().type == 'DELIM':
            self.consume('DELIM', '.')
            col = self.consume('ID').value
            return f"{name}.{col}"
        return name

    def parse_projection_expr(self) -> Expr:
        """Parse a SELECT-list item: may be a function call, column ref, or literal."""
        return self.parse_expression()

    def parse_expression(self):
        return self.parse_or()

    def parse_or(self):
        node = self.parse_and()
        while self.peek().value == 'OR':
            op    = self.consume().value
            right = self.parse_and()
            node  = BinaryOp(node, op, right)
        return node

    def parse_and(self):
        node = self.parse_comparison()
        while self.peek().value == 'AND':
            op    = self.consume().value
            right = self.parse_comparison()
            node  = BinaryOp(node, op, right)
        return node

    def parse_comparison(self):
        node = self.parse_additive()
        if self.peek().type == 'OP' and self.peek().value != '*':
            op    = self.consume().value
            right = self.parse_additive()
            return BinaryOp(node, op, right)
        return node

    def parse_additive(self):
        node = self.parse_primary()
        while self.peek().type == 'OP' and self.peek().value in ('+', '-'):
            op    = self.consume().value
            right = self.parse_primary()
            node  = BinaryOp(node, op, right)
        return node

    def parse_primary(self):
        token = self.peek()

        # Numeric literal
        if token.type == 'NUMBER':
            raw = self.consume().value
            return Literal(float(raw) if '.' in raw else int(raw))

        # String literal
        if token.type == 'STRING':
            return Literal(self.consume().value.strip("'"))

        # NULL
        if token.value == 'NULL':
            self.consume()
            return Literal(None)

        # Function call: KEYWORD that is a function name followed by '('
        if token.type == 'KEYWORD' and token.value in _FUNC_NAMES:
            func_name = self.consume().value
            self.consume('DELIM', '(')
            args = []
            while self.peek().value != ')':
                args.append(self.parse_expression())
                if self.peek().value == ',':
                    self.consume('DELIM', ',')
            self.consume('DELIM', ')')
            return FuncCall(func_name=func_name, args=args)

        # Identifier — may be bare name, qualified name, or function call (user-defined IDs)
        if token.type == 'ID':
            name = self.consume().value
            # function call using an ID token (e.g. custom func)
            if self.peek().value == '(' and self.peek().type == 'DELIM':
                self.consume('DELIM', '(')
                args = []
                while self.peek().value != ')':
                    args.append(self.parse_expression())
                    if self.peek().value == ',':
                        self.consume('DELIM', ',')
                self.consume('DELIM', ')')
                return FuncCall(func_name=name.upper(), args=args)
            # qualified name: table.col
            if self.peek().value == '.' and self.peek().type == 'DELIM':
                self.consume('DELIM', '.')
                col = self.consume('ID').value
                return ColumnRef(f"{name}.{col}")
            return ColumnRef(name)

        # Parenthesised expression
        if token.value == '(':
            self.consume('DELIM', '(')
            expr = self.parse_expression()
            self.consume('DELIM', ')')
            return expr

        self.error(f"Unexpected token in expression: {token.value!r}")


# =============================================================================
# 4. EXECUTOR  (delegates to engine for DML/SELECT; DDL handled inline)
# =============================================================================

def execute(stmt, engine, out=None) -> None:
    """Execute a parsed statement against *engine*, writing output to *out*."""
    if out is None:
        out = sys.stdout

    if isinstance(stmt, CreateTableStmt):
        schema = [{'name': c.name, 'type': c.data_type} for c in stmt.columns]
        engine.create_table(stmt.table_name, schema)
        print(f"Table '{stmt.table_name}' created.", file=out)
        return

    if isinstance(stmt, _DropTableStmt):
        engine.drop_table(stmt.table_name)
        print(f"Table '{stmt.table_name}' dropped.", file=out)
        return

    # All DML/query statements go through the execution engine
    from engine import build_plan, run_plan
    plan = build_plan(stmt, engine)
    run_plan(plan, stmt, engine, out)


# =============================================================================
# 5. CLI
# =============================================================================

def main():
    # When running as __main__, register this module under its real name so
    # that engine.py's `import db_cli` resolves to the same module object.
    # Without this, isinstance checks in engine.py fail against __main__ classes.
    import sys as _sys
    _sys.modules.setdefault('db_cli', _sys.modules[__name__])

    parser = argparse.ArgumentParser(
        description='Flat-DB: SQL parser + query execution engine CLI',
    )
    parser.add_argument('--query',     required=True, help='SQL query string')
    parser.add_argument('--debug-ast', action='store_true',
                        help='Print AST as JSON and exit (no execution)')
    parser.add_argument('--execute',   action='store_true',
                        help='Execute the query against the storage engine')
    parser.add_argument('--db-dir',    default='./data',
                        help='Data directory for the storage engine (default: ./data)')

    args = parser.parse_args()

    try:
        lexer      = Lexer(args.query)
        sql_parser = Parser(lexer.tokens, args.query)
        ast        = sql_parser.parse()

        if args.debug_ast:
            print(json.dumps(asdict(ast), indent=2))
            sys.exit(0)

        if args.execute:
            from storage import StorageEngine
            db_engine = StorageEngine(args.db_dir)
            execute(ast, db_engine)
        else:
            print("Query parsed successfully.")

        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
