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
    name: str
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
class ColumnDef:
    name: str
    data_type: str

@dataclass
class Statement:
    type: str

@dataclass
class CreateTableStmt(Statement):
    table_name: str
    columns: List[ColumnDef]
    type: str

@dataclass
class InsertStmt(Statement):
    table_name: str
    values: List[Expr]
    type: str

@dataclass
class SelectStmt(Statement):
    from_table: str
    selection: Optional[Expr] = None
    limit: Optional[int] = None
    type: str

# =============================================================================
# 2. LEXER
# =============================================================================

TOKEN_SPEC = [
    ('NUMBER',   r'-?\d+'),
    ('STRING',   r"'(?:''|[^'])*'"),
    ('KEYWORD',  r'\b(SELECT|FROM|WHERE|CREATE|TABLE|INSERT|INTO|VALUES|LIMIT|AND|OR|INT|BIGINT|TEXT|NULL)\b'),
    ('ID',       r'[a-zA-Z_][a-zA-Z0-9_]*'),
    ('OP',       r'!=|<=|>=|[=<>*]'),
    ('DELIM',    r'[(),;]'),
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
            kind = mo.lastgroup
            value = mo.group()
            pos = mo.start()
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

class Parser:
    def __init__(self, tokens, raw_query):
        self.tokens = tokens
        self.raw_query = raw_query
        self.pos = 0

    def peek(self):
        return self.tokens[self.pos]

    def consume(self, expected_type=None, expected_value=None):
        token = self.peek()
        if expected_type and token.type != expected_type:
            self.error(f"Expected {expected_type}, got {token.type}")
        if expected_value and token.value.upper() != expected_value.upper():
            self.error(f"Expected '{expected_value}', got '{token.value}'")
        self.pos += 1
        return token

    def error(self, message):
        token = self.peek()
        line_preview = self.raw_query[max(0, token.pos-10):token.pos+10]
        full_msg = f"Syntax Error at position {token.pos}: {message}\nNear: ...{line_preview}..."
        print(full_msg, file=sys.stderr)
        sys.exit(1)

    def parse(self):
        token = self.peek()
        if token.value == 'CREATE': return self.parse_create()
        if token.value == 'INSERT': return self.parse_insert()
        if token.value == 'SELECT': return self.parse_select()
        self.error(f"Unsupported statement start: {token.value}")

    # --- CREATE TABLE ---
    def parse_create(self):
        self.consume('KEYWORD', 'CREATE')
        self.consume('KEYWORD', 'TABLE')
        table_name = self.consume('ID').value
        self.consume('DELIM', '(')
        columns = []
        while True:
            col_name = self.consume('ID').value
            col_type = self.consume('KEYWORD').value
            if col_type not in ['INT', 'BIGINT', 'TEXT']:
                self.error(f"Unsupported data type {col_type}")
            columns.append(ColumnDef(col_name, col_type))
            if self.peek().value == ')': break
            self.consume('DELIM', ',')
        self.consume('DELIM', ')')
        return CreateTableStmt(table_name=table_name, columns=columns, type="CREATE_TABLE")

    # --- INSERT INTO ---
    def parse_insert(self):
        self.consume('KEYWORD', 'INSERT')
        self.consume('KEYWORD', 'INTO')
        table_name = self.consume('ID').value
        self.consume('KEYWORD', 'VALUES')
        self.consume('DELIM', '(')
        values = []
        while True:
            values.append(self.parse_expression())
            if self.peek().value == ')': break
            self.consume('DELIM', ',')
        self.consume('DELIM', ')')
        return InsertStmt(table_name=table_name, values=values,type="INSERT")

    # --- SELECT ---
    def parse_select(self):
        self.consume('KEYWORD', 'SELECT')
        
        # Projections
        projections = []
        if self.peek().value == '*':
            self.consume('OP', '*')
            projections = "*"
        else:
            while True:
                projections.append(self.consume('ID').value)
                if self.peek().value != ',': break
                self.consume('DELIM', ',')
        
        if not projections: self.error("SELECT requires at least one column or '*'")

        self.consume('KEYWORD', 'FROM')
        table_name = self.consume('ID').value
        
        where_clause = None
        if self.peek().value == 'WHERE':
            self.consume('KEYWORD', 'WHERE')
            where_clause = self.parse_expression()
            
        limit_val = None
        if self.peek().value == 'LIMIT':
            self.consume('KEYWORD', 'LIMIT')
            limit_val = int(self.consume('NUMBER').value)

        return SelectStmt(projections, table_name, where_clause, limit_val)

    # --- EXPRESSION PARSING (Precedence: OR < AND < Comparison) ---
    def parse_expression(self):
        return self.parse_or()

    def parse_or(self):
        node = self.parse_and()
        while self.peek().value == 'OR':
            op = self.consume().value
            right = self.parse_and()
            node = BinaryOp(node, op, right)
        return node

    def parse_and(self):
        node = self.parse_comparison()
        while self.peek().value == 'AND':
            op = self.consume().value
            right = self.parse_comparison()
            node = BinaryOp(node, op, right)
        return node

    def parse_comparison(self):
        node = self.parse_primary()
        if self.peek().type == 'OP' and self.peek().value != '*':
            op = self.consume().value
            right = self.parse_primary()
            return BinaryOp(node, op, right)
        return node

    def parse_primary(self):
        token = self.peek()
        if token.type == 'NUMBER':
            return Literal(int(self.consume().value))
        if token.type == 'STRING':
            return Literal(self.consume().value.strip("'"))
        if token.value == 'NULL':
            self.consume()
            return Literal(None)
        if token.type == 'ID':
            return ColumnRef(self.consume().value)
        if token.value == '(':
            self.consume('DELIM', '(')
            expr = self.parse_expression()
            self.consume('DELIM', ')')
            return expr
        self.error(f"Unexpected token in expression: {token.value}")

# =============================================================================
# 4. CLI INTEGRATION
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="SQL Parser CLI")
    parser.add_argument("--query", required=True, help="SQL query string to parse")
    parser.add_argument("--debug-ast", action="store_true", help="Output AST in JSON format")
    
    args = parser.parse_args()
    
    try:
        lexer = Lexer(args.query)
        sql_parser = Parser(lexer.tokens, args.query)
        ast = sql_parser.parse()
        
        if args.debug_ast:
            print(json.dumps(asdict(ast), indent=2))
        else:
            print("Query parsed successfully.")
        
        sys.exit(0)
    except Exception as e:
        # Unexpected errors (Lexer errors or Logic errors)
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()