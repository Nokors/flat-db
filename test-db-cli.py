import unittest
from db_cli import Lexer, Parser, SelectStmt, CreateTableStmt, InsertStmt, BinaryOp

class TestSQLParser(unittest.TestCase):

    def parse_query(self, query):
        lexer = Lexer(query)
        parser = Parser(lexer.tokens, query)
        return parser.parse()

    # --- SUCCESS CASES ---

    def test_create_table_success(self):
        sql = "CREATE TABLE users (id INT, name TEXT)"
        ast = self.parse_query(sql)
        self.assertIsInstance(ast, CreateTableStmt)
        self.assertEqual(ast.table_name, "users")
        self.assertEqual(len(ast.columns), 3)
        self.assertEqual(ast.columns[1].name, "name")
        self.assertEqual(ast.columns[1].data_type, "TEXT")

    def test_insert_success(self):
        sql = "INSERT INTO orders VALUES (101, 'Widget', NULL)"
        ast = self.parse_query(sql)
        self.assertIsInstance(ast, InsertStmt)
        self.assertEqual(ast.table_name, "orders")
        self.assertEqual(len(ast.values), 3)
        self.assertEqual(ast.values[2].value, None) # NULL test

    def test_select_star_success(self):
        sql = "SELECT * FROM items"
        ast = self.parse_query(sql)
        self.assertIsInstance(ast, SelectStmt)
        self.assertEqual(ast.projections, "*")

    def test_complex_where_clause(self):
        sql = "SELECT id FROM users WHERE age >= 18 AND status = 'active' LIMIT 10"
        ast = self.parse_query(sql)
        self.assertIsInstance(ast, SelectStmt)
        self.assertEqual(ast.limit, 10)
        # Verify logical operator nesting
        self.assertIsInstance(ast.selection, BinaryOp)
        self.assertEqual(ast.selection.op, "AND")

    # --- ERROR CASES ---

    def test_syntax_error_missing_from(self):
        sql = "SELECT name users" # Missing FROM
        with self.assertRaises(SystemExit):
            self.parse_query(sql)

    def test_syntax_error_invalid_type(self):
        sql = "CREATE TABLE t (a TIMESTAMP)" # Unsupported type
        with self.assertRaises(SystemExit):
            self.parse_query(sql)

    def test_syntax_error_unclosed_paren(self):
        sql = "INSERT INTO t VALUES (1, 2"
        with self.assertRaises(SystemExit):
            self.parse_query(sql)

if __name__ == '__main__':
    unittest.main()