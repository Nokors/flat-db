import argparse
import sys
import json
import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where, Comparison, Parenthesis
from sqlparse.tokens import Keyword, DML, DDL, String, Number, Punctuation, Whitespace
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Union, Any

# ==========================================
# 1. Internal AST Structure
# ==========================================

@dataclass
class Expr:
    type: str

@dataclass
class LiteralExpr(Expr):
    value: Any

@dataclass
class ColumnRefExpr(Expr):
    column_name: str

@dataclass
class BinaryOpExpr(Expr):
    left: Expr
    op: str
    right: Expr

@dataclass
class ColumnDef:
    name: str
    data_type: str

@dataclass
class AstNode:
    statement_type: str

@dataclass
class CreateTableStmt(AstNode):
    table_name: str
    columns: List[ColumnDef]

@dataclass
class InsertStmt(AstNode):
    table_name: str
    values: List[Expr]

@dataclass
class SelectStmt(AstNode):
    projections: List[str]  # ["col1", "col2"] or ["*"]
    from_table: str
    selection: Optional[Expr] = None
    limit: Optional[int] = None

# ==========================================
# 2. Parsing Subsystem (Adapter)
# ==========================================

class SQLParseError(Exception):
    pass

class ASTAdapter:
    def transform(self, sql: str) -> AstNode:
        parsed = sqlparse.parse(sql)
        if not parsed:
            raise SQLParseError("Empty query.")
        
        stmt = parsed[0]
        root_type = stmt.get_type()

        if root_type == "CREATE":
            return self._parse_create(stmt)
        elif root_type == "INSERT":
            return self._parse_insert(stmt)
        elif root_type == "SELECT":
            return self._parse_select(stmt)
        else:
            raise SQLParseError(f"Unsupported statement type: {root_type}")

    def _parse_create(self, stmt):
        tokens = [t for t in stmt.tokens if not t.is_whitespace]
        # Minimal syntax: CREATE TABLE <name> (<defs>)
        table_name = ""
        columns = []
        
        for i, token in enumerate(tokens):
            if isinstance(token, Identifier):
                table_name = token.get_real_name()
            if isinstance(token, Parenthesis):
                # Inner tokens of (col1 INT, col2 TEXT)
                inner_text = token.value.strip("()")
                parts = inner_text.split(",")
                for p in parts:
                    col_parts = p.strip().split()
                    if len(col_parts) < 2:
                        raise SQLParseError(f"Invalid column definition: {p}")
                    columns.append(ColumnDef(name=col_parts[0], data_type=col_parts[1].upper()))
        
        if not table_name: raise SQLParseError("Missing table name in CREATE.")
        return CreateTableStmt("CREATE_TABLE", table_name, columns)

    def _parse_insert(self, stmt):
        tokens = [t for t in stmt.tokens if not t.is_whitespace]
        table_name = ""
        values = []

        for token in tokens:
            if isinstance(token, Identifier):
                table_name = token.get_real_name()
            if isinstance(token, Parenthesis):
                # Clean up values like (1, 'Alice')
                val_tokens = sqlparse.parse(token.value.strip("()"))[0].tokens
                for vt in val_tokens:
                    if vt.ttype in (Number.Integer, String.Single):
                        val = vt.value.strip("'")
                        values.append(LiteralExpr("Literal", val))
        
        return InsertStmt("INSERT", table_name, values)

    def _parse_select(self, stmt):
        tokens = [t for t in stmt.tokens if not t.is_whitespace]
        projections = []
        from_table = ""
        selection = None
        limit = None

        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            
            # 1. Projections
            if token.ttype == DML and token.value.upper() == "SELECT":
                idx += 1
                next_t = tokens[idx]
                if next_t.value == "*":
                    projections = ["*"]
                elif isinstance(next_t, IdentifierList):
                    projections = [i.get_real_name() for i in next_t.get_identifiers()]
                elif isinstance(next_t, Identifier):
                    projections = [next_t.get_real_name()]
            
            # 2. FROM
            elif token.ttype == Keyword and token.value.upper() == "FROM":
                idx += 1
                from_table = tokens[idx].get_real_name()

            # 3. WHERE
            elif isinstance(token, Where):
                selection = self._parse_where(token)

            # 4. LIMIT
            elif token.ttype == Keyword and token.value.upper() == "LIMIT":
                idx += 1
                limit = int(tokens[idx].value)

            idx += 1

        return SelectStmt("SELECT", projections, from_table, selection, limit)

    def _parse_where(self, where_clause):
        # Simplification: looks for basic comparison A = B
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                left = ColumnRefExpr("ColumnRef", token.left.value)
                op = token.value.split()[1] # simplistic split for '='
                right = LiteralExpr("Literal", token.right.value.strip("'"))
                return BinaryOpExpr("BinaryOp", left, op, right)
        return None

# ==========================================
# 3. CLI Logic
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="DB-CLI SQL Parser")
    parser.add_argument("--query", required=True, help="SQL query to parse")
    parser.add_argument("--debug-ast", action="store_true", help="Print the internal AST")

    args = parser.parse_args()
    adapter = ASTAdapter()

    try:
        ast = adapter.transform(args.query)
        
        if args.debug_ast:
            # Convert dataclass to dict for pretty JSON output
            print(json.dumps(asdict(ast), indent=2))
        
        sys.exit(0)

    except Exception as e:
        print(f"Syntax Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()