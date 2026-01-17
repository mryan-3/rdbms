package sql

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/mryan-3/rdbms/internal/storage"
)

type Executor struct {
	db *storage.Database
}

func NewExecutor(db *storage.Database) *Executor {
	return &Executor{db: db}
}

type Result struct {
	Columns      []string
	Rows         [][]string
	RowsAffected int
	Message      string
}

func (e *Executor) Execute(stmt Node) (*Result, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return e.executeSelect(s)
	case *InsertStatement:
		return e.executeInsert(s)
	case *UpdateStatement:
		return e.executeUpdate(s)
	case *DeleteStatement:
		return e.executeDelete(s)
	case *CreateTableStatement:
		return e.executeCreateTable(s)
	case *DropTableStatement:
		return e.executeDropTable(s)
	case *BeginTransactionStatement, *CommitStatement, *RollbackStatement:
		return &Result{Message: s.String()}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) resolveColumnIndex(colRef *ColumnRef, tables map[string]*storage.Table, offsets map[string]int) (int, error) {
	if colRef.Table != "" {
		// Specific table referenced (e.g., "users.id" or "u.id")
		table, ok := tables[colRef.Table]
		if !ok {
			return -1, fmt.Errorf("unknown table or alias: %s", colRef.Table)
		}
		offset := offsets[colRef.Table]
		colIdx := table.Schema.ColumnIndex(colRef.Column)
		if colIdx < 0 {
			return -1, fmt.Errorf("column %s not found in table %s", colRef.Column, colRef.Table)
		}
		return offset + colIdx, nil
	}

	// No table specified, search all tables (e.g., "id")
	foundIdx := -1
	for name, table := range tables {
		colIdx := table.Schema.ColumnIndex(colRef.Column)
		if colIdx >= 0 {
			if foundIdx != -1 {
				return -1, fmt.Errorf("ambiguous column name: %s", colRef.Column)
			}
			foundIdx = offsets[name] + colIdx
		}
	}

	if foundIdx == -1 {
		return -1, fmt.Errorf("column not found: %s", colRef.Column)
	}

	return foundIdx, nil
}

func (e *Executor) executeSelect(stmt *SelectStatement) (*Result, error) {
	if len(stmt.Tables) == 0 {
		return nil, fmt.Errorf("no table specified in SELECT")
	}

	// 1. Initialize context for potentially multiple tables
	primaryTableRef := stmt.Tables[0]
	primaryTable, err := e.db.GetTable(primaryTableRef.Name)
	if err != nil {
		return nil, err
	}

	tableMap := make(map[string]*storage.Table)
	offsetMap := make(map[string]int)
	currentOffset := 0
	
	// Register primary table (using both name and potential alias)
	lookupName := primaryTableRef.Name
	if primaryTableRef.Alias != "" {
		lookupName = primaryTableRef.Alias
	}
	
	tableMap[lookupName] = primaryTable
	offsetMap[lookupName] = 0
	currentOffset += len(primaryTable.Schema.Columns)

	var intermediateRows []*storage.Row
	
	primaryRows := primaryTable.Select(nil)
	for _, r := range primaryRows {
		intermediateRows = append(intermediateRows, r.Clone())
	}

	// 2. Process Joins
	for _, join := range stmt.Joins {
		targetTable, err := e.db.GetTable(join.Table)
		if err != nil {
			return nil, err
		}

		lookupName := join.Table
		if join.Alias != "" {
			lookupName = join.Alias
		}
		
		tableMap[lookupName] = targetTable
		offsetMap[lookupName] = currentOffset
		
		targetColsLen := len(targetTable.Schema.Columns)
		
		newRows := make([]*storage.Row, 0)

		targetRows := targetTable.Select(nil)

		for _, leftRow := range intermediateRows {
			matchFound := false

			for _, rightRow := range targetRows {
				combinedValues := make([]storage.Value, len(leftRow.Values)+len(rightRow.Values))
				copy(combinedValues, leftRow.Values)
				copy(combinedValues[len(leftRow.Values):], rightRow.Values)
				combinedRow := storage.NewRow(combinedValues)

				matches := true
				if len(join.Conditions) > 0 {
					for _, cond := range join.Conditions {
						val, err := e.evaluateExpressionForJoinedRow(cond, combinedRow, tableMap, offsetMap)
						if err != nil || !e.getValueAsBool(val) {
							matches = false
							break
						}
					}
				}

				if matches {
					newRows = append(newRows, combinedRow)
					matchFound = true
				}
			}

			// Handle LEFT JOIN: if no match found, append row with NULLs for the right table
			if !matchFound && (join.Type == "LEFT" || join.Type == "LEFT OUTER") {
				combinedValues := make([]storage.Value, len(leftRow.Values)+targetColsLen)
				copy(combinedValues, leftRow.Values)
				for k := 0; k < targetColsLen; k++ {
					combinedValues[len(leftRow.Values)+k] = storage.NullValue{}
				}
				newRows = append(newRows, storage.NewRow(combinedValues))
			}
		}

		intermediateRows = newRows
		currentOffset += targetColsLen
	}

	// 3. Apply WHERE clause on the fully joined rows
	finalRows := make([]*storage.Row, 0)
	for _, row := range intermediateRows {
		if stmt.Where != nil {
			val, err := e.evaluateExpressionForJoinedRow(stmt.Where, row, tableMap, offsetMap)
			if err != nil {
				return nil, err
			}
			if !e.getValueAsBool(val) {
				continue
			}
		}
		finalRows = append(finalRows, row)
	}

	// 4. Project Results
	result := &Result{
		Columns: stmt.Columns,
		Rows:    make([][]string, 0),
	}
	
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		result.Columns = make([]string, 0)
		
		for _, col := range primaryTable.Schema.Columns {
			result.Columns = append(result.Columns, col.Name) 
		}
		for _, join := range stmt.Joins {
			if join.Alias != "" {
				tbl, _ := e.db.GetTable(join.Table)
				for _, col := range tbl.Schema.Columns {
					result.Columns = append(result.Columns, col.Name)
				}
			} else {
				tbl, _ := e.db.GetTable(join.Table)
				for _, col := range tbl.Schema.Columns {
					result.Columns = append(result.Columns, col.Name)
				}
			}
		}
	}

	for _, row := range finalRows {
		rowStringValues := make([]string, 0)
		for _, colName := range result.Columns {
			colRef := &ColumnRef{Column: colName}
			
			var tablePart, colPart string
			dotIdx := -1
			for i, char := range colName {
				if char == '.' {
					dotIdx = i
					break
				}
			}
			
			if dotIdx != -1 {
				tablePart = colName[:dotIdx]
				colPart = colName[dotIdx+1:]
				colRef = &ColumnRef{Table: tablePart, Column: colPart}
			} else {
				colRef = &ColumnRef{Column: colName}
			}
			
			idx, err := e.resolveColumnIndex(colRef, tableMap, offsetMap)
			if err != nil {
				return nil, err
			}
			
			val, _ := row.Get(idx)
			rowStringValues = append(rowStringValues, val.ToString())
		}
		result.Rows = append(result.Rows, rowStringValues)
	}

	// 5. Limit and Offset
	if stmt.Limit != nil && len(result.Rows) > 0 {
		limit := *stmt.Limit
		offset := 0
		if stmt.Offset != nil {
			offset = *stmt.Offset
		}
		
		if offset >= len(result.Rows) {
			result.Rows = make([][]string, 0)
		} else {
			end := offset + limit
			if end > len(result.Rows) {
				end = len(result.Rows)
			}
			result.Rows = result.Rows[offset:end]
		}
	}

	return result, nil
}

func (e *Executor) executeInsert(stmt *InsertStatement) (*Result, error) {
	table, err := e.db.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	result := &Result{
		RowsAffected: 0,
	}

	for _, rowExprs := range stmt.Values {
		rowValues := make([]storage.Value, len(table.Schema.Columns))

		if len(stmt.Columns) > 0 {
			colToExpr := make(map[string]Expression)
			for i, colName := range stmt.Columns {
				if i < len(rowExprs) {
					colToExpr[colName] = rowExprs[i]
				}
			}

			for i, colDef := range table.Schema.Columns {
				if expr, exists := colToExpr[colDef.Name]; exists {
					val, err := e.evaluateExpression(expr, table)
					if err != nil {
						return nil, err
					}
					rowValues[i] = val
				} else {
					rowValues[i] = storage.NullValue{}
				}
			}
		} else {
			for i := range table.Schema.Columns {
				if i < len(rowExprs) {
					val, err := e.evaluateExpression(rowExprs[i], table)
					if err != nil {
						return nil, err
					}
					rowValues[i] = val
				} else {
					rowValues[i] = storage.NullValue{}
				}
			}
		}

		row := storage.NewRow(rowValues)
		_, err := table.Insert(row)
		if err != nil {
			return nil, err
		}
		result.RowsAffected++
	}

	result.Message = fmt.Sprintf("%d row(s) inserted", result.RowsAffected)
	return result, nil
}

func (e *Executor) executeUpdate(stmt *UpdateStatement) (*Result, error) {
	table, err := e.db.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	result := &Result{
		RowsAffected: 0,
	}

	predicate := e.buildPredicate(stmt.Where, table)

	updater := func(row *storage.Row) {
		updates := make(map[string]storage.Value)
		for _, setClause := range stmt.SetClauses {
			val, err := e.evaluateExpression(setClause.Value, table)
			if err != nil {
				return
			}
			updates[setClause.Column] = val
		}

		for colName, val := range updates {
			colIdx := table.Schema.ColumnIndex(colName)
			if colIdx >= 0 {
				row.Set(colIdx, val)
			}
		}
	}

	updated, err := table.Update(predicate, updater)
	if err != nil {
		return nil, err
	}

	result.RowsAffected = updated
	result.Message = fmt.Sprintf("%d row(s) updated", updated)
	return result, nil
}

func (e *Executor) executeDelete(stmt *DeleteStatement) (*Result, error) {
	table, err := e.db.GetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	result := &Result{
		RowsAffected: 0,
	}

	predicate := e.buildPredicate(stmt.Where, table)

	deleted, err := table.Delete(predicate)
	if err != nil {
		return nil, err
	}

	result.RowsAffected = deleted
	result.Message = fmt.Sprintf("%d row(s) deleted", deleted)
	return result, nil
}

func (e *Executor) executeCreateTable(stmt *CreateTableStatement) (*Result, error) {
	schema := storage.NewSchema()

	for _, colDef := range stmt.Columns {
		dataType, err := e.parseDataType(colDef.Type)
		if err != nil {
			return nil, fmt.Errorf("invalid data type %s for column %s: %w", colDef.Type, colDef.Name, err)
		}

		col := storage.NewColumn(colDef.Name, dataType, colDef.Primary, colDef.Unique, colDef.NotNull)

		if colDef.Default != nil {
			defaultValue, err := e.evaluateExpression(*colDef.Default, nil)
			if err != nil {
				return nil, fmt.Errorf("error evaluating default value for column %s: %w", colDef.Name, err)
			}
			col.Default = defaultValue
		}

		schema.AddColumn(col)
	}

	err := e.db.CreateTable(stmt.Table, schema)
	if err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("Table %s created", stmt.Table)}, nil
}

func (e *Executor) executeDropTable(stmt *DropTableStatement) (*Result, error) {
	err := e.db.DropTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("Table %s dropped", stmt.Table)}, nil
}

func (e *Executor) parseDataType(typeName string) (storage.DataType, error) {
	switch typeName {
	case "INTEGER":
		return storage.TypeInteger, nil
	case "TEXT", "VARCHAR", "STRING":
		return storage.TypeText, nil
	case "FLOAT", "REAL", "DOUBLE":
		return storage.TypeFloat, nil
	case "BOOLEAN", "BOOL":
		return storage.TypeBoolean, nil
	default:
		return 0, fmt.Errorf("unsupported data type: %s", typeName)
	}
}

func (e *Executor) buildPredicate(expr Expression, table *storage.Table) func(*storage.Row) bool {
	if expr == nil {
		return func(row *storage.Row) bool { return true }
	}

	return func(row *storage.Row) bool {
		val, err := e.evaluateExpressionForRow(expr, table, row)
		if err != nil {
			return false
		}

		if boolVal, ok := val.(*storage.BooleanValue); ok {
			return boolVal.Value
		}

		return false
	}
}

func (e *Executor) evaluateExpression(expr Expression, table *storage.Table) (storage.Value, error) {
	return e.evaluateExpressionForRow(expr, table, nil)
}

func (e *Executor) evaluateExpressionForRow(expr Expression, table *storage.Table, row *storage.Row) (storage.Value, error) {
	switch expr := expr.(type) {
	case *LiteralExpression:
		return expr.parseLiteral()
	case *NullLiteral:
		return storage.NullValue{}, nil
	case *ColumnRef:
		if row == nil {
			return nil, fmt.Errorf("cannot evaluate column reference without row context")
		}
		colIdx := table.Schema.ColumnIndex(expr.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column not found: %s", expr.Column)
		}
		return row.Get(colIdx)
	case *BinaryExpression:
		left, err := e.evaluateExpressionForRow(expr.Left, table, row)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExpressionForRow(expr.Right, table, row)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, expr.Op, right)
	case *UnaryExpression:
		right, err := e.evaluateExpressionForRow(expr.Right, table, row)
		if err != nil {
			return nil, err
		}
		return e.evaluateUnaryOp(expr.Op, right)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) evaluateExpressionForJoinedRow(expr Expression, row *storage.Row, tables map[string]*storage.Table, offsets map[string]int) (storage.Value, error) {
	switch expr := expr.(type) {
	case *LiteralExpression:
		return expr.parseLiteral()
	case *NullLiteral:
		return storage.NullValue{}, nil
	case *ColumnRef:
		if row == nil {
			return nil, fmt.Errorf("cannot evaluate column reference without row context")
		}
		idx, err := e.resolveColumnIndex(expr, tables, offsets)
		if err != nil {
			return nil, err
		}
		return row.Get(idx)
	case *BinaryExpression:
		left, err := e.evaluateExpressionForJoinedRow(expr.Left, row, tables, offsets)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateExpressionForJoinedRow(expr.Right, row, tables, offsets)
		if err != nil {
			return nil, err
		}
		return e.evaluateBinaryOp(left, expr.Op, right)
	case *UnaryExpression:
		right, err := e.evaluateExpressionForJoinedRow(expr.Right, row, tables, offsets)
		if err != nil {
			return nil, err
		}
		return e.evaluateUnaryOp(expr.Op, right)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *LiteralExpression) parseLiteral() (storage.Value, error) {
	if isNumericLiteral(e.Value) {
		if containsDecimal(e.Value) {
			f, err := strconv.ParseFloat(e.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float literal: %s", e.Value)
			}
			return storage.NewFloatValue(f), nil
		} else {
			i, err := strconv.ParseInt(e.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer literal: %s", e.Value)
			}
			return storage.NewIntegerValue(i), nil
		}
	}

	lower := toLower(e.Value)
	if lower == "true" {
		return storage.NewBooleanValue(true), nil
	}
	if lower == "false" {
		return storage.NewBooleanValue(false), nil
	}

	return storage.NewTextValue(e.Value), nil
}

func isNumericLiteral(s string) bool {
	match, _ := regexp.MatchString(`^-?\d+\.?\d*$`, s)
	return match
}

func containsDecimal(s string) bool {
	return regexp.MustCompile(`\.`).MatchString(s)
}

func toLower(s string) string {
	return regexp.MustCompile(`(?i)[A-Z]`).ReplaceAllStringFunc(s, func(c string) string {
		return string([]byte{toLowerChar(c[0])})
	})
}

func toLowerChar(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func (e *Executor) evaluateBinaryOp(left storage.Value, op string, right storage.Value) (storage.Value, error) {
	switch op {
	case "=", "==":
		return storage.NewBooleanValue(left.Equals(right)), nil
	case "!=", "<>":
		return storage.NewBooleanValue(!left.Equals(right)), nil
	case "<":
		return storage.NewBooleanValue(left.LessThan(right)), nil
	case "<=":
		return storage.NewBooleanValue(left.LessThan(right) || left.Equals(right)), nil
	case ">":
		return storage.NewBooleanValue(!left.LessThan(right) && !left.Equals(right)), nil
	case ">=":
		return storage.NewBooleanValue(!left.LessThan(right)), nil
	case "AND":
		leftBool := e.getValueAsBool(left)
		rightBool := e.getValueAsBool(right)
		return storage.NewBooleanValue(leftBool && rightBool), nil
	case "OR":
		leftBool := e.getValueAsBool(left)
		rightBool := e.getValueAsBool(right)
		return storage.NewBooleanValue(leftBool || rightBool), nil
	case "+", "-", "*", "/":
		return e.evaluateArithmeticOp(left, op, right)
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}
}

func (e *Executor) evaluateUnaryOp(op string, right storage.Value) (storage.Value, error) {
	switch op {
	case "NOT":
		return storage.NewBooleanValue(!e.getValueAsBool(right)), nil
	case "-":
		switch v := right.(type) {
		case *storage.IntegerValue:
			return storage.NewIntegerValue(-v.Value), nil
		case *storage.FloatValue:
			return storage.NewFloatValue(-v.Value), nil
		default:
			return nil, fmt.Errorf("unary minus not supported for type %T", right)
		}
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", op)
	}
}

func (e *Executor) evaluateArithmeticOp(left storage.Value, op string, right storage.Value) (storage.Value, error) {
	switch l := left.(type) {
	case *storage.IntegerValue:
		switch r := right.(type) {
		case *storage.IntegerValue:
			switch op {
			case "+":
				return storage.NewIntegerValue(l.Value + r.Value), nil
			case "-":
				return storage.NewIntegerValue(l.Value - r.Value), nil
			case "*":
				return storage.NewIntegerValue(l.Value * r.Value), nil
			case "/":
				if r.Value == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return storage.NewIntegerValue(l.Value / r.Value), nil
			}
		case *storage.FloatValue:
			switch op {
			case "+":
				return storage.NewFloatValue(float64(l.Value) + r.Value), nil
			case "-":
				return storage.NewFloatValue(float64(l.Value) - r.Value), nil
			case "*":
				return storage.NewFloatValue(float64(l.Value) * r.Value), nil
			case "/":
				if r.Value == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return storage.NewFloatValue(float64(l.Value) / r.Value), nil
			}
		}
	case *storage.FloatValue:
		switch r := right.(type) {
		case *storage.IntegerValue:
			switch op {
			case "+":
				return storage.NewFloatValue(l.Value + float64(r.Value)), nil
			case "-":
				return storage.NewFloatValue(l.Value - float64(r.Value)), nil
			case "*":
				return storage.NewFloatValue(l.Value * float64(r.Value)), nil
			case "/":
				if r.Value == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return storage.NewFloatValue(l.Value / float64(r.Value)), nil
			}
		case *storage.FloatValue:
			switch op {
			case "+":
				return storage.NewFloatValue(l.Value + r.Value), nil
			case "-":
				return storage.NewFloatValue(l.Value - r.Value), nil
			case "*":
				return storage.NewFloatValue(l.Value * r.Value), nil
			case "/":
				if r.Value == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return storage.NewFloatValue(l.Value / r.Value), nil
			}
		}
	}

	return nil, fmt.Errorf("arithmetic operation not supported for types %T and %T", left, right)
}

func (e *Executor) getValueAsBool(v storage.Value) bool {
	switch val := v.(type) {
	case *storage.BooleanValue:
		return val.Value
	case *storage.IntegerValue:
		return val.Value != 0
	case *storage.FloatValue:
		return val.Value != 0
	case *storage.TextValue:
		return val.Value != ""
	default:
		return false
	}
}
