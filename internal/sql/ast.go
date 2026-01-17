package sql

import (
	"fmt"
)

type NodeType int

const (
	NodeSelectStmt NodeType = iota
	NodeInsertStmt
	NodeUpdateStmt
	NodeDeleteStmt
	NodeCreateTableStmt
	NodeDropTableStmt
	NodeBeginTransactionStmt
	NodeCommitStmt
	NodeRollbackStmt
)

type Node interface {
	Type() NodeType
	String() string
}

type SelectStatement struct {
	Columns  []string
	Tables   []TableRef
	Where    Expression
	Joins    []*JoinClause
	OrderBy  []OrderByClause
	Limit    *int
	Offset   *int
	Distinct bool
}

type TableRef struct {
	Name  string
	Alias string
}

func (t TableRef) String() string {
	if t.Alias != "" {
		return fmt.Sprintf("%s AS %s", t.Name, t.Alias)
	}
	return t.Name
}

func (s *SelectStatement) Type() NodeType { return NodeSelectStmt }
func (s *SelectStatement) String() string {
	result := "SELECT "
	if s.Distinct {
		result += "DISTINCT "
	}
	for i, col := range s.Columns {
		if i > 0 {
			result += ", "
		}
		result += col
	}
	result += " FROM "
	for i, table := range s.Tables {
		if i > 0 {
			result += ", "
		}
		result += table.String()
	}
	if s.Where != nil {
		result += " WHERE " + s.Where.String()
	}
	for _, join := range s.Joins {
		result += " " + join.String()
	}
	if len(s.OrderBy) > 0 {
		result += " ORDER BY"
		for i, ob := range s.OrderBy {
			if i > 0 {
				result += ","
			}
			result += " " + ob.String()
		}
	}
	if s.Limit != nil {
		result += fmt.Sprintf(" LIMIT %d", *s.Limit)
	}
	if s.Offset != nil {
		result += fmt.Sprintf(" OFFSET %d", *s.Offset)
	}
	return result
}

type JoinClause struct {
	Type       string
	Table      string
	Alias      string
	Conditions []Expression
}

func (j *JoinClause) String() string {
	result := fmt.Sprintf("%s JOIN %s", j.Type, j.Table)
	if j.Alias != "" {
		result += fmt.Sprintf(" AS %s", j.Alias)
	}
	if len(j.Conditions) > 0 {
		result += " ON "
		for i, cond := range j.Conditions {
			if i > 0 {
				result += " AND "
			}
			result += cond.String()
		}
	}
	return result
}

type OrderByClause struct {
	Column string
	Asc    bool
}

func (o *OrderByClause) String() string {
	result := o.Column
	if !o.Asc {
		result += " DESC"
	}
	return result
}

type InsertStatement struct {
	Table   string
	Columns []string
	Values  [][]Expression
}

func (s *InsertStatement) Type() NodeType { return NodeInsertStmt }
func (s *InsertStatement) String() string {
	result := fmt.Sprintf("INSERT INTO %s", s.Table)
	if len(s.Columns) > 0 {
		result += " ("
		for i, col := range s.Columns {
			if i > 0 {
				result += ", "
			}
			result += col
		}
		result += ")"
	}
	result += " VALUES "
	for i, row := range s.Values {
		if i > 0 {
			result += ", "
		}
		result += "("
		for j, val := range row {
			if j > 0 {
				result += ", "
			}
			result += val.String()
		}
		result += ")"
	}
	return result
}

type UpdateStatement struct {
	Table      string
	SetClauses []SetClause
	Where      Expression
}

type SetClause struct {
	Column string
	Value  Expression
}

func (s *SetClause) String() string {
	return fmt.Sprintf("%s = %s", s.Column, s.Value.String())
}

func (s *UpdateStatement) Type() NodeType { return NodeUpdateStmt }
func (s *UpdateStatement) String() string {
	result := fmt.Sprintf("UPDATE %s SET", s.Table)
	for i, set := range s.SetClauses {
		if i > 0 {
			result += ", "
		}
		result += " " + set.String()
	}
	if s.Where != nil {
		result += " WHERE " + s.Where.String()
	}
	return result
}

type DeleteStatement struct {
	Table string
	Where Expression
}

func (s *DeleteStatement) Type() NodeType { return NodeDeleteStmt }
func (s *DeleteStatement) String() string {
	result := fmt.Sprintf("DELETE FROM %s", s.Table)
	if s.Where != nil {
		result += " WHERE " + s.Where.String()
	}
	return result
}

type CreateTableStatement struct {
	Table       string
	Columns     []ColumnDefinition
	ForeignKeys []ForeignKeyDefinition
}

type ColumnDefinition struct {
	Name    string
	Type    string
	Primary bool
	Unique  bool
	NotNull bool
	Default *Expression
}

type ForeignKeyDefinition struct {
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

func (s *CreateTableStatement) Type() NodeType { return NodeCreateTableStmt }
func (s *CreateTableStatement) String() string {
	result := fmt.Sprintf("CREATE TABLE %s (", s.Table)
	for i, col := range s.Columns {
		if i > 0 {
			result += ", "
		}
		result += col.Name + " " + col.Type
		if col.Primary {
			result += " PRIMARY KEY"
		}
		if col.Unique {
			result += " UNIQUE"
		}
		if col.NotNull {
			result += " NOT NULL"
		}
	}
	result += ")"
	return result
}

type DropTableStatement struct {
	Table string
}

func (s *DropTableStatement) Type() NodeType { return NodeDropTableStmt }
func (s *DropTableStatement) String() string {
	return fmt.Sprintf("DROP TABLE %s", s.Table)
}

type BeginTransactionStatement struct{}

func (s *BeginTransactionStatement) Type() NodeType { return NodeBeginTransactionStmt }
func (s *BeginTransactionStatement) String() string {
	return "BEGIN TRANSACTION"
}

type CommitStatement struct{}

func (s *CommitStatement) Type() NodeType { return NodeCommitStmt }
func (s *CommitStatement) String() string {
	return "COMMIT"
}

type RollbackStatement struct{}

func (s *RollbackStatement) Type() NodeType { return NodeRollbackStmt }
func (s *RollbackStatement) String() string {
	return "ROLLBACK"
}

type Expression interface {
	String() string
}

type BinaryExpression struct {
	Left  Expression
	Op    string
	Right Expression
}

func (e *BinaryExpression) String() string {
	return fmt.Sprintf("%s %s %s", e.Left.String(), e.Op, e.Right.String())
}

type UnaryExpression struct {
	Op    string
	Right Expression
}

func (e *UnaryExpression) String() string {
	return fmt.Sprintf("%s %s", e.Op, e.Right.String())
}

type ColumnRef struct {
	Table  string
	Column string
}

func (e *ColumnRef) String() string {
	if e.Table != "" {
		return fmt.Sprintf("%s.%s", e.Table, e.Column)
	}
	return e.Column
}

type LiteralExpression struct {
	Value string
}

func (e *LiteralExpression) String() string {
	return e.Value
}

type NullLiteral struct{}

func (e *NullLiteral) String() string {
	return "NULL"
}

type FunctionCall struct {
	Name      string
	Arguments []Expression
}

func (e *FunctionCall) String() string {
	result := e.Name + "("
	for i, arg := range e.Arguments {
		if i > 0 {
			result += ", "
		}
		result += arg.String()
	}
	result += ")"
	return result
}
