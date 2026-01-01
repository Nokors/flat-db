# SQL AST Parser CLI (db-cli)

A standalone Python CLI tool designed to parse a specific subset of SQL into a custom, semantically meaningful **Abstract Syntax Tree (AST)**. 

This project uses `sqlparse` for initial tokenization and a custom **Adapter Layer** to transform those tokens into a strictly typed internal model, ensuring the AST is decoupled from the underlying library.

---

## Features

* **Standalone CLI**: Process queries using the `--query` flag.
* **Typed AST Nodes**: Maps queries to specific Python Dataclasses (e.g., `SelectStmt`, `InsertStmt`).
* **Validation**: Performs structural checks and returns non-zero exit codes on syntax errors.
* **Debug Mode**: Use `--debug-ast` to output a pretty-printed JSON representation of the internal query model.
* **Error Diagnostics**: Clear error messages sent to `stderr` for unsupported SQL.

---

## Installation

1. **Requirement**: Python 3.7+
2. **Install dependencies**:
   ```bash
   pip install requirements.txt
3. **Run**:
   ```bash
   pip install requirements.txt

## Run
1. ***Successful CREATE TABLE:***
   ```bash
    python3 db_cli.py --debug-ast --query "CREATE TABLE users (id INT, name TEXT)"

2. ***SELECT with WHERE and LIMITP:***
   ```bash
   python3 db_cli.py --debug-ast --query "SELECT name, age FROM users WHERE age > 18 AND status = 'active' LIMIT 5"
3. ***Handling Syntax Errors:***
   ```bash
   python3 db_cli.py --query "SELECT FROM users"
# Output: Syntax Error at position 7: Expected ID, got KEYWORD
# Near: ...SELECT FROM users...

## Unit test
```bash
 python3 -m unittest test-db-cli.py