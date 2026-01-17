# RDBMS Architecture Documentation

## Overview

The RDBMS is built with a clean, layered architecture separating concerns between storage, SQL processing, and user interaction.

## Component Diagram

```
┌─────────────────────────────────────────────────────┐
│                   User Interface                  │
│  ┌─────────┐            ┌──────────────┐       │
│  │   CLI   │            │  Web App    │       │
│  │ (REPL)  │            │  (HTTP)      │       │
│  └────┬────┘            └──────┬───────┘       │
│       │                        │                │
└───────┼────────────────────────┼────────────────┘
        │                        │
        ▼                        ▼
┌─────────────────────────────────────────────────────┐
│                  SQL Layer                      │
│  ┌──────────┐    ┌──────────┐   ┌──────┐ │
│  │  Lexer   │───▶│  Parser  │───▶│ Exec │ │
│  └──────────┘    └──────────┘   └──────┘ │
└──────────────────────┼───────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│               Database Engine                    │
│  ┌──────────┐    ┌──────────┐   ┌──────┐ │
│  │ Catalog  │    │  Tables  │   │ FKs  │ │
│  └──────────┘    └──────────┘   └──────┘ │
└──────────────────────┼───────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│               Storage Engine                    │
│  ┌──────────┐    ┌──────────┐   ┌──────┐ │
│  │  Rows    │    │ B-Tree   │   │Index │ │
│  └──────────┘    └──────────┘   └──────┘ │
└─────────────────────────────────────────────────────┘
```

## Layer Details

### 1. Storage Engine (`internal/storage/`)

#### Types System
- **Value Interface**: Base type for all values (Integer, Float, Text, Boolean, Null)
- **Type Safety**: Runtime type checking with proper coercion
- **Value Operations**: Comparison, cloning, string conversion

#### B-Tree Index
- **Balanced Tree**: Self-balancing with configurable order (default 4)
- **Operations**:
  - Insert: O(log n) with split at overflow
  - Lookup: O(log n) binary search within nodes
  - Delete: O(log n) with redistribution/merge
  - Range: O(k log n) for k results

#### Table Management
- **Schema**: Column definitions with constraints (PK, UNIQUE, NOT NULL)
- **Row Storage**: In-memory array with concurrent access
- **Index Registry**: Automatic index creation for PK/UNIQUE columns
- **Constraint Enforcement**: Primary key, unique, and foreign key validation

#### Database Catalog
- **Table Registry**: Map of table names to Table objects
- **Foreign Key Management**: Cascading operations
- **Concurrency**: Global RWMutex for safe concurrent access

### 2. SQL Layer (`internal/sql/`)

#### Lexer
- **Token Types**: Keywords, identifiers, literals, operators, punctuation
- **Features**:
  - String literal support with escape handling
  - Numeric literals (int, float)
  - Comment support (-- single line)
  - Error recovery with position tracking

#### Parser
- **Strategy**: Recursive descent with precedence climbing
- **Grammar Coverage**:
  - SELECT: Columns, FROM, WHERE, JOIN, ORDER BY, LIMIT/OFFSET, DISTINCT
  - INSERT: Column specification, multi-row VALUES
  - UPDATE: SET clauses with WHERE
  - DELETE: WHERE clause
  - CREATE TABLE: Column definitions with constraints
  - DROP TABLE

- **Error Handling**: Detailed error messages with suggestions
- **AST**: Type-safe node hierarchy for queries

#### Executor
- **Execution Model**:
  - Build predicates from WHERE expressions
  - Table scans with filter application
  - Result projection (column selection)
  - Limit/offset application

- **Expression Evaluation**:
  - Comparison operators (=, !=, <, >, <=, >=)
  - Logical operators (AND, OR, NOT)
  - Arithmetic operators (+, -, *, /)
  - Column references
  - Literals (including NULL)

- **Type Coercion**: Automatic type conversion for compatible types

### 3. REPL Interface (`internal/repl/`)

#### Commands
- **Meta Commands**: `\d`, `\dt`, `\s`, `\import`, `\export`, `\help`, `\quit`
- **SQL Commands**: Full SQL language support

#### Features
- **Pretty Tables**: Formatted result display with column alignment
- **History**: Command execution tracking
- **File I/O**: Import/export SQL scripts
- **Error Display**: Contextual error messages

### 4. Web Application (`webapp/`)

#### Architecture
- **HTTP Server**: Built with net/http standard library
- **Handlers**: RESTful endpoints for CRUD operations
- **Templates**: HTML rendering with text/template

#### Routes
- `GET /`: Main dashboard
- `GET /users/new`: User creation form
- `POST /users/create`: Create user
- `GET /tasks/new`: Task creation form
- `POST /tasks/create`: Create task
- `GET /users/delete`: Delete user
- `GET /tasks/delete`: Delete task

#### Database Operations
- **JOIN Queries**: Tasks with assigned users via LEFT JOIN
- **CRUD Operations**: Full Create, Read, Update, Delete
- **Constraint Handling**: Unique email constraint, foreign key references

## Data Flow Examples

### SELECT Query

```
User Input: "SELECT name FROM users WHERE id > 5"

1. Lexer: Tokenize SQL
   [SELECT, name, FROM, users, WHERE, id, >, 5]

2. Parser: Build AST
   SelectStatement{
     Columns: ["name"],
     Tables: ["users"],
     Where: BinaryExpression{
       Left: ColumnRef{id},
       Op: ">",
       Right: LiteralExpression{5}
     }
   }

3. Executor: Build predicate
   predicate = func(row) {
     return row.Get(id) > 5
   }

4. Storage: Scan and filter
   rows = table.Select(predicate)

5. Project columns
   result = rows.map(row => [row.Get(name)])

6. Format output
   Display table with rows
```

### JOIN Query

```
User Input: "SELECT t.title, u.name FROM tasks t LEFT JOIN users u ON t.user_id = u.id"

1. Parser: Parse with JOIN support
   SelectStatement{
     Tables: ["tasks"],
     Joins: [{
       Type: "LEFT",
       Table: "users",
       Alias: "u",
       Conditions: [BinaryExpression{t.user_id, =, u.id}]
     }]
   }

2. Executor: Execute nested loop join
   For each task in tasks:
     Find matching user by user_id
     Combine columns
     Add to result

3. Return combined rows
```

### INSERT with Constraints

```
User Input: "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"

1. Parser: Parse INSERT statement
   InsertStatement{
     Table: "users",
     Columns: ["name", "email"],
     Values: [["John", "john@example.com"]]
   }

2. Executor: Validate and insert
   - Create Row with values
   - Validate column count matches schema
   - Validate NOT NULL constraints
   - Validate UNIQUE constraint (check index)
   - Update B-tree indexes
   - Append to table

3. Success or error
   Return "1 row(s) inserted" or constraint error
```

## Concurrency Model

### Lock Strategy
- **Database-level RWMutex**: Protects table catalog
- **Table-level RWMutex**: Protects individual tables
- **Index-level RWMutex**: Protects B-tree structures

### Lock Acquisition Order
1. Database lock (for catalog operations)
2. Table lock (for table operations)
3. Index locks (acquired during operations)

### Safety Guarantees
- **Write exclusion**: Only one writer at a time per table
- **Read concurrency**: Multiple readers can access simultaneously
- **No deadlocks**: Global lock ordering prevents circular wait

## Performance Characteristics

### Time Complexities
- **Index Lookup**: O(log n)
- **Table Scan**: O(n)
- **Insert**: O(log n) for index + O(1) for row append
- **Update**: O(log n) for index + O(1) for row update
- **Delete**: O(log n) for index + O(1) for row delete
- **JOIN (Nested Loop)**: O(n * m) where n, m are table sizes

### Memory Usage
- **Row Storage**: O(n * m) where n = rows, m = avg row size
- **Index Storage**: O(n * log_k(n)) where k is B-tree order
- **Schema Metadata**: O(t * c) where t = tables, c = avg columns

### Scalability Considerations
- **In-memory**: Limited by available RAM
- **B-tree Fanout**: Higher order = shallower tree
- **Index Selectivity**: Index on high-cardinality columns best

## Future Improvements

### Short-term
- Disk persistence with write-ahead logging
- Query plan optimization (index selection)
- Statistics for cardinality estimation

### Medium-term
- MVCC for true concurrent transactions
- Statement-level prepared queries
- Connection pooling

### Long-term
- Query cache for repeated queries
- Partitioning support
- Distributed architecture
