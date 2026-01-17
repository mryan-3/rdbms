package repl

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mryan-3/rdbms/internal/sql"
	"github.com/mryan-3/rdbms/internal/storage"
)

type REPL struct {
	db      *storage.Database
	lexer   *sql.Lexer
	parser  *sql.Parser
	exec    *sql.Executor
	scanner *bufio.Scanner
}

func NewREPL(db *storage.Database) *REPL {
	return &REPL{
		db:      db,
		exec:    sql.NewExecutor(db),
		scanner: bufio.NewScanner(os.Stdin),
	}
}

func (r *REPL) Run() error {
	fmt.Println("RDBMS Interactive SQL Shell")
	fmt.Println("Type 'help' for available commands, 'quit' to exit")
	fmt.Println()

	for {
		fmt.Print("rdbms> ")

		if !r.scanner.Scan() {
			fmt.Println()
			break
		}

		input := strings.TrimSpace(r.scanner.Text())

		if input == "" {
			continue
		}

		if input == "quit" || input == "exit" || input == "\\q" {
			fmt.Println("Goodbye!")
			return nil
		}

		if err := r.handleCommand(input); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}

	return r.scanner.Err()
}

func (r *REPL) handleCommand(input string) error {
	lowerInput := strings.ToLower(input)

	switch lowerInput {
	case "help", "\\h", "?":
		r.printHelp()
		return nil

	case "\\d":
		r.listTables()
		return nil

	case "\\dt":
		r.listTables()
		return nil

	case "\\t", "\\tables":
		r.listTables()
		return nil

	case "\\s", "\\schema":
		r.listSchema()
		return nil

	case "\\version", "\\v":
		fmt.Println("RDBMS v1.0.0 - A simple relational database management system")
		return nil

	case "\\clear", "\\c":
		fmt.Print("\033[H\033[2J")
		return nil
	}

	if strings.HasPrefix(lowerInput, "\\d ") {
		tableName := strings.TrimSpace(input[3:])
		r.DescribeTable(tableName)
		return nil
	}

	if strings.HasPrefix(lowerInput, "\\import ") {
		filePath := strings.TrimSpace(input[8:])
		return r.ImportFile(filePath)
	}

	if strings.HasPrefix(lowerInput, "\\export ") {
		filePath := strings.TrimSpace(input[8:])
		return r.ExportFile(filePath)
	}

	return r.ExecuteSQL(input)
}

func (r *REPL) printResult(result *sql.Result) {
	if result.Message != "" {
		fmt.Println(result.Message)
	}

	if len(result.Rows) > 0 {
		r.printTable(result.Columns, result.Rows)
	}
}

func (r *REPL) printTable(columns []string, rows [][]string) {
	if len(rows) == 0 {
		fmt.Println("Empty set")
		return
	}

	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	for _, row := range rows {
		for i, val := range row {
			if len(val) > colWidths[i] {
				colWidths[i] = len(val)
			}
		}
	}

	totalWidth := 0
	for _, w := range colWidths {
		totalWidth += w + 3
	}

	fmt.Println(strings.Repeat("-", totalWidth))

	fmt.Print("| ")
	for i, col := range columns {
		fmt.Printf("%-*s | ", colWidths[i], col)
	}
	fmt.Println()

	fmt.Println(strings.Repeat("-", totalWidth))

	for _, row := range rows {
		fmt.Print("| ")
		for i, val := range row {
			fmt.Printf("%-*s | ", colWidths[i], val)
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("-", totalWidth))
	fmt.Printf("%d row(s) in set\n", len(rows))
}

func (r *REPL) printHelp() {
	help := `
Available Commands:

Meta Commands:
  \h, \?, help          Show this help message
  \q, quit, exit        Exit the REPL
  \d                    List all tables
  \d [table]            Describe table schema
  \dt, \t, \tables      List all tables
  \s, \schema           Show full database schema
  \version, \v          Show version information
  \clear, \c            Clear the screen
  \import [file]        Import SQL from file
  \export [file]        Export database to SQL file

SQL Commands:
  CREATE TABLE          Create a new table
  DROP TABLE            Drop a table
  SELECT                Query data
  INSERT                Insert data
  UPDATE                Update data
  DELETE                Delete data
  BEGIN TRANSACTION     Start a transaction
  COMMIT                Commit transaction
  ROLLBACK              Rollback transaction

Examples:
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
  INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
  SELECT * FROM users WHERE name = 'John Doe';
  UPDATE users SET email = 'new@example.com' WHERE id = 1;
  DELETE FROM users WHERE id = 1;
`
	fmt.Println(help)
}

func (r *REPL) listTables() {
	tables := r.db.ListTables()
	if len(tables) == 0 {
		fmt.Println("No tables found")
		return
	}

	fmt.Println("\nList of tables:")
	for _, table := range tables {
		tbl, _ := r.db.GetTable(table)
		fmt.Printf("  %s (%d rows)\n", table, tbl.Count())
	}
}

func (r *REPL) listSchema() {
	tables := r.db.ListTables()
	if len(tables) == 0 {
		fmt.Println("No tables found")
		return
	}

	fmt.Println("\nDatabase Schema:")
	for _, tableName := range tables {
		table, _ := r.db.GetTable(tableName)
		fmt.Printf("\nTable: %s\n", tableName)
		fmt.Printf("  Columns:\n")
		for _, col := range table.Schema.Columns {
			constraints := ""
			if col.PrimaryKey {
				constraints += "PRIMARY KEY"
			}
			if col.Unique {
				constraints += "UNIQUE"
			}
			if col.NotNull {
				constraints += "NOT NULL"
			}
			fmt.Printf("    %s %s%s\n", col.Name, col.Type.String(), constraints)
		}
		fmt.Printf("  Rows: %d\n", table.Count())
	}
}

func (r *REPL) DescribeTable(tableName string) {
	table, err := r.db.GetTable(tableName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("\nTable: %s\n", tableName)
	fmt.Println("Columns:")
	fmt.Println("  Name      | Type    | Constraints")
	fmt.Println("  ----------|---------|------------")

	for _, col := range table.Schema.Columns {
		constraints := ""
		if col.PrimaryKey {
			constraints = "PRIMARY KEY"
		} else if col.Unique {
			constraints = "UNIQUE"
		}
		if col.NotNull {
			if constraints != "" {
				constraints += ", "
			}
			constraints += "NOT NULL"
		}

		fmt.Printf("  %-9s | %-7s | %s\n", col.Name, col.Type.String(), constraints)
	}

	fmt.Printf("\nIndexes: %d\n", len(table.Indexes))
	for colName := range table.Indexes {
		fmt.Printf("  - %s\n", colName)
	}

	fmt.Printf("\nForeign Keys: %d\n", len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		fmt.Printf("  - %s -> %s(%s) ON DELETE %s\n",
			strings.Join(fk.Columns, ", "),
			fk.RefTable,
			strings.Join(fk.RefColumns, ", "),
			fk.OnDelete)
	}

	fmt.Printf("\nRows: %d\n", table.Count())
}

func (r *REPL) ExportFile(filePath string) error {
	var builder strings.Builder

	tables := r.db.ListTables()
	for _, tableName := range tables {
		table, _ := r.db.GetTable(tableName)

		builder.WriteString(fmt.Sprintf("CREATE TABLE %s (", tableName))
		for i, col := range table.Schema.Columns {
			if i > 0 {
				builder.WriteString(", ")
			}
			constraints := ""
			if col.PrimaryKey {
				constraints = " PRIMARY KEY"
			} else if col.Unique {
				constraints = " UNIQUE"
			}
			if col.NotNull {
				constraints += " NOT NULL"
			}
			builder.WriteString(fmt.Sprintf("%s %s%s", col.Name, col.Type.String(), constraints))
		}
		builder.WriteString(");\n")

		for _, row := range table.Rows {
			values := make([]string, row.Len())
			for i := 0; i < row.Len(); i++ {
				val, _ := row.Get(i)
				if val.Type() == storage.TypeText {
					values[i] = fmt.Sprintf("'%s'", val.ToString())
				} else {
					values[i] = val.ToString()
				}
			}
			builder.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (%s);\n", tableName, strings.Join(values, ", ")))
		}

		builder.WriteString("\n")
	}

	err := os.WriteFile(filePath, []byte(builder.String()), 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Exported database to %s\n", filePath)
	return nil
}
