#!/bin/bash

echo "=== RDBMS Quick Start Demo ==="
echo ""
echo "1. Building binaries..."
make build > /dev/null 2>&1
echo "   Built CLI and web app"
echo ""
echo "2. Testing CLI with SQL..."
./bin/rdbms << 'EOF' 2>&1 | head -20
CREATE TABLE demo (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER
);

INSERT INTO demo VALUES (1, 'First', 100);
INSERT INTO demo VALUES (2, 'Second', 200);
INSERT INTO demo VALUES (3, 'Third', 300);

SELECT * FROM demo WHERE value > 150;
\d demo
\quit
EOF

echo ""
echo "=== Demo Complete ==="
echo ""
echo "To continue exploring:"
echo "  - Run CLI: ./bin/rdbms"
echo "  - Run web app: ./bin/webapp (then visit http://localhost:8080)"
echo ""
echo "Example SQL commands:"
echo "  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
echo "  INSERT INTO users (name) VALUES ('John Doe');"
echo "  SELECT * FROM users WHERE name = 'John Doe';"
echo "  UPDATE users SET name = 'Jane' WHERE id = 1;"
echo "  DELETE FROM users WHERE id = 1;"
