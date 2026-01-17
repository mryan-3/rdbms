package storage

import (
	"fmt"
	"strconv"
)

type DataType int

const (
	TypeInteger DataType = iota
	TypeFloat
	TypeText
	TypeBoolean
	TypeNull
)

func (dt DataType) String() string {
	switch dt {
	case TypeInteger:
		return "INTEGER"
	case TypeFloat:
		return "FLOAT"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeNull:
		return "NULL"
	default:
		return "UNKNOWN"
	}
}

type Value interface {
	Type() DataType
	ToString() string
	Equals(other Value) bool
	LessThan(other Value) bool
	Clone() Value
}

type NullValue struct{}

func (n NullValue) Type() DataType   { return TypeNull }
func (n NullValue) ToString() string { return "NULL" }
func (n NullValue) Equals(other Value) bool {
	_, ok := other.(NullValue)
	return ok
}
func (n NullValue) LessThan(other Value) bool { return false }
func (n NullValue) Clone() Value              { return NullValue{} }

type IntegerValue struct {
	Value int64
}

func NewIntegerValue(v int64) *IntegerValue {
	return &IntegerValue{Value: v}
}

func (i *IntegerValue) Type() DataType { return TypeInteger }
func (i *IntegerValue) ToString() string {
	return strconv.FormatInt(i.Value, 10)
}
func (i *IntegerValue) Equals(other Value) bool {
	if o, ok := other.(*IntegerValue); ok {
		return i.Value == o.Value
	}
	return false
}
func (i *IntegerValue) LessThan(other Value) bool {
	if o, ok := other.(*IntegerValue); ok {
		return i.Value < o.Value
	}
	return false
}
func (i *IntegerValue) Clone() Value {
	return &IntegerValue{Value: i.Value}
}

type FloatValue struct {
	Value float64
}

func NewFloatValue(v float64) *FloatValue {
	return &FloatValue{Value: v}
}

func (f *FloatValue) Type() DataType { return TypeFloat }
func (f *FloatValue) ToString() string {
	return strconv.FormatFloat(f.Value, 'f', -1, 64)
}
func (f *FloatValue) Equals(other Value) bool {
	if o, ok := other.(*FloatValue); ok {
		return f.Value == o.Value
	}
	return false
}
func (f *FloatValue) LessThan(other Value) bool {
	if o, ok := other.(*FloatValue); ok {
		return f.Value < o.Value
	}
	return false
}
func (f *FloatValue) Clone() Value {
	return &FloatValue{Value: f.Value}
}

type TextValue struct {
	Value string
}

func NewTextValue(v string) *TextValue {
	return &TextValue{Value: v}
}

func (t *TextValue) Type() DataType { return TypeText }
func (t *TextValue) ToString() string {
	return t.Value
}
func (t *TextValue) Equals(other Value) bool {
	if o, ok := other.(*TextValue); ok {
		return t.Value == o.Value
	}
	return false
}
func (t *TextValue) LessThan(other Value) bool {
	if o, ok := other.(*TextValue); ok {
		return t.Value < o.Value
	}
	return false
}
func (t *TextValue) Clone() Value {
	return &TextValue{Value: t.Value}
}

type BooleanValue struct {
	Value bool
}

func NewBooleanValue(v bool) *BooleanValue {
	return &BooleanValue{Value: v}
}

func (b *BooleanValue) Type() DataType { return TypeBoolean }
func (b *BooleanValue) ToString() string {
	return strconv.FormatBool(b.Value)
}
func (b *BooleanValue) Equals(other Value) bool {
	if o, ok := other.(*BooleanValue); ok {
		return b.Value == o.Value
	}
	return false
}
func (b *BooleanValue) LessThan(other Value) bool {
	if o, ok := other.(*BooleanValue); ok {
		return !b.Value && o.Value
	}
	return false
}
func (b *BooleanValue) Clone() Value {
	return &BooleanValue{Value: b.Value}
}

func ParseValue(dataType DataType, s string) (Value, error) {
	switch dataType {
	case TypeInteger:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", s)
		}
		return NewIntegerValue(v), nil
	case TypeFloat:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", s)
		}
		return NewFloatValue(v), nil
	case TypeText:
		return NewTextValue(s), nil
	case TypeBoolean:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean: %s", s)
		}
		return NewBooleanValue(v), nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", dataType)
	}
}

type Column struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	Unique     bool
	NotNull    bool
	Default    Value
}

func NewColumn(name string, dataType DataType, primaryKey, unique, notNull bool) *Column {
	return &Column{
		Name:       name,
		Type:       dataType,
		PrimaryKey: primaryKey,
		Unique:     unique,
		NotNull:    notNull,
	}
}

type Schema struct {
	Columns []*Column
}

func NewSchema() *Schema {
	return &Schema{Columns: make([]*Column, 0)}
}

func (s *Schema) AddColumn(col *Column) {
	s.Columns = append(s.Columns, col)
}

func (s *Schema) GetColumn(name string) (*Column, bool) {
	for _, col := range s.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return nil, false
}

func (s *Schema) ColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (s *Schema) PrimaryKeyColumns() []*Column {
	pks := make([]*Column, 0)
	for _, col := range s.Columns {
		if col.PrimaryKey {
			pks = append(pks, col)
		}
	}
	return pks
}

func (s *Schema) String() string {
	result := ""
	for i, col := range s.Columns {
		if i > 0 {
			result += ", "
		}
		result += col.Name + " " + col.Type.String()
		if col.PrimaryKey {
			result += " PRIMARY KEY"
		}
		if col.Unique {
			result += " UNIQUE"
		}
		if col.NotNull {
			result += " NOT NULL"
		}
	}
	return result
}
