# RDBMS - Go-Based Relational Database Engine

Welcome to the RDBMS project. This repository contains a fully functional, in-memory relational database management system written in Go. It is designed as an educational and foundational implementation of core database concepts, including B-Tree indexing, SQL parsing, and query execution.

## System Architecture

The system follows a classic layered architecture, separating the query language processing from the underlying storage engine.

### 1. Storage Engine (internal/storage)
The bedrock of the system. It handles data persistence (in-memory), schema enforcement, and low-level data structures.
- Tables: Row-oriented storage protected by RWMutexes for concurrent access safety.
- Indexes: Implements a B-Tree data structure for efficient O(log n) lookups on Primary and Unique keys.
- Type System: Strictly typed values (Integer, Text, Float, Boolean) with runtime type checking during insertions.
- Constraints: Enforces PRIMARY KEY, UNIQUE, NOT NULL, and FOREIGN KEY constraints at the engine level.

### 2. SQL Engine (internal/sql)
Responsible for interpreting and executing SQL commands.
- Lexer: Tokenizes raw SQL strings into a stream of known tokens (Keywords, Identifiers, Literals).
- Parser: A recursive descent parser that constructs an Abstract Syntax Tree (AST). It handles complex grammar including JOIN clauses, nested expressions, and operator precedence.
- Executor: Traverses the AST to perform operations against the storage engine.
    - Query Execution: Implements full table scans and nested-loop joins.
    - Expression Evaluation: Supports arithmetic, logical (AND/OR), and comparison operators against row data.

### 3. Interfaces
- CLI / REPL (cmd/rdbms): An interactive shell for direct database manipulation.
- Web App (webapp/): A demonstration application (Task Manager) showcasing CRUD operations and JOIN capabilities.

---

## Getting Started

### Prerequisites
- Go 1.18 or higher
- Make (optional, for build commands)

### Installation

Clone the repository:
```bash
git clone https://github.com/mryan-3/rdbms.git
cd rdbms
```

### Building the Project

We provide a Makefile for convenience, or you can use standard Go commands.

```bash
# Build both the CLI and Web Server binaries
make build
# OR
go build -o bin/rdbms cmd/rdbms/main.go
go build -o bin/webapp webapp/main.go
```

### Running the REPL

The Interactive REPL is the best way to explore the database engine.

```bash
./bin/rdbms
```

Supported Commands:
- \d: List all tables.
- \d <table>: Describe table schema (columns, indexes, foreign keys).
- \s: Show full schema.
- \import <file>: Import SQL commands from a file.
- SQL Statements: Standard SQL (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP).

### Running the Web Demo

The web application demonstrates a real-world use case (Task Management System) utilizing relations between Users and Tasks.

```bash
./bin/webapp
```
Open http://localhost:8080 in your browser.

---

## Code Walkthrough for Contributors

If you are joining the team, here is what you need to know about the codebase:

### Handling SQL Execution (internal/sql/executor.go)
This is where the magic happens. The executeSelect method is particularly important.
- Joins: We currently implement a Nested Loop Join. When a SELECT involves multiple tables, we iterate through the primary table and loop through the joined tables to find matching rows.
- Column Resolution: Since we support JOINs, columns can be ambiguous (e.g., id vs users.id). The executor uses a resolveColumnIndex helper to map qualified names to the correct position in the "virtual" joined row.

### The Parser (internal/sql/parser.go)
We use a standard recursive descent parser.
- If you need to add a new SQL keyword, start by adding it to the Lexer in lexer.go.
- Then, implement the parsing logic in parser.go (e.g., parseSelect, parseExpression).
- Finally, update the AST definitions in ast.go.

### Storage & Concurrency (internal/storage/)
- Concurrency: The database is thread-safe for concurrent reads. Writes utilize a global or table-level lock.
- B-Trees: We use B-Trees for indexing. If you touch btree.go, ensure you understand node splitting and rebalancing logic.

---

## Feature Support

| Feature | Status | Notes |
|---------|--------|-------|
| Data Types | Supported | INTEGER, TEXT, FLOAT, BOOLEAN |
| CRUD | Supported | Full support (INSERT, SELECT, UPDATE, DELETE) |
| Filtering | Supported | WHERE with AND, OR, NOT, comparisons |
| Joins | Supported | INNER, LEFT, RIGHT (Nested Loop implementation) |
| Constraints | Supported | PK, UNIQUE, NOT NULL, FK (Cascade/Restrict) |
| Indexing | Supported | B-Tree on PK and Unique columns |
| Transactions | In Progress | AST support exists; Engine logic pending |
| Persistence | Unsupported | In-memory only (Disk I/O planned) |

## Contributing

1. Code Style: We follow standard Go conventions. Run go fmt before committing.
2. Testing: This project is educational but aims for stability. Add tests for new features.
3. PRs: Please keep PRs focused on single features or fixes.

---
