package repl

import (
	"fmt"
	"os"
	"strings"

	"github.com/mryan-3/rdbms/internal/sql"
)

func (r *REPL) ExecuteSQL(input string) error {
	r.lexer = sql.NewLexer(input)
	r.parser = sql.NewParser(r.lexer)

	stmt, err := r.parser.Parse()
	if err != nil {
		return err
	}

	result, err := r.exec.Execute(stmt)
	if err != nil {
		return err
	}

	r.printResult(result)
	return nil
}

func (r *REPL) ImportFile(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	statements := strings.Split(string(content), ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := r.ExecuteSQL(stmt); err != nil {
			return fmt.Errorf("error executing statement: %w", err)
		}
	}

	fmt.Printf("Imported %d statements from %s\n", len(statements), filePath)
	return nil
}
