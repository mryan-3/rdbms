package storage

import (
	"fmt"
	"sync"
)

type Row struct {
	Values []Value
}

func NewRow(values []Value) *Row {
	return &Row{Values: values}
}

func (r *Row) Get(index int) (Value, error) {
	if index < 0 || index >= len(r.Values) {
		return nil, fmt.Errorf("index out of bounds: %d", index)
	}
	return r.Values[index], nil
}

func (r *Row) Set(index int, value Value) error {
	if index < 0 || index >= len(r.Values) {
		return fmt.Errorf("index out of bounds: %d", index)
	}
	r.Values[index] = value
	return nil
}

func (r *Row) Clone() *Row {
	values := make([]Value, len(r.Values))
	for i, v := range r.Values {
		values[i] = v.Clone()
	}
	return &Row{Values: values}
}

func (r *Row) Len() int {
	return len(r.Values)
}

func (r *Row) String() string {
	result := "("
	for i, v := range r.Values {
		if i > 0 {
			result += ", "
		}
		result += v.ToString()
	}
	result += ")"
	return result
}

type Table struct {
	Name        string
	Schema      *Schema
	Rows        []*Row
	Indexes     map[string]Index
	RowIDSeq    int
	ForeignKeys []*ForeignKey
	mu          sync.RWMutex
}

type ForeignKey struct {
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

const (
	FKActionCascade  = "CASCADE"
	FKActionRestrict = "RESTRICT"
	FKActionSetNull  = "SET NULL"
	FKActionNoAction = "NO ACTION"
)

func NewTable(name string, schema *Schema) *Table {
	return &Table{
		Name:        name,
		Schema:      schema,
		Rows:        make([]*Row, 0),
		Indexes:     make(map[string]Index),
		RowIDSeq:    1,
		ForeignKeys: make([]*ForeignKey, 0),
	}
}

func (t *Table) AddIndex(columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.Schema.GetColumn(columnName); !exists {
		return fmt.Errorf("column %s not found", columnName)
	}

	if _, exists := t.Indexes[columnName]; exists {
		return fmt.Errorf("index on column %s already exists", columnName)
	}

	index := NewIndex()
	t.Indexes[columnName] = index

	colIndex := t.Schema.ColumnIndex(columnName)
	for rowID, row := range t.Rows {
		if val, err := row.Get(colIndex); err == nil {
			index.Insert(val, rowID)
		}
	}

	return nil
}

func (t *Table) RemoveIndex(columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.Indexes[columnName]; !exists {
		return fmt.Errorf("index on column %s not found", columnName)
	}

	delete(t.Indexes, columnName)
	return nil
}

func (t *Table) Insert(row *Row) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Handle auto-incrementing primary key
	pkColIndex := -1
	for i, col := range t.Schema.Columns {
		if col.PrimaryKey {
			pkColIndex = i
			break
		}
	}

	isPKNull := false
	if pkColIndex != -1 {
		if pkColIndex < len(row.Values) {
			if val, err := row.Get(pkColIndex); err == nil && val.Type() == TypeNull {
				isPKNull = true
			}
		} else {
			// PK column is not present in the insert statement
			isPKNull = true
		}
	}

	if isPKNull {
		// Ensure the row has enough capacity for the PK
		if pkColIndex >= len(row.Values) {
			newValues := make([]Value, pkColIndex+1)
			copy(newValues, row.Values)
			for i := len(row.Values); i <= pkColIndex; i++ {
				newValues[i] = NullValue{}
			}
			row = NewRow(newValues)
		}
		// Assign the next sequence value
		row.Set(pkColIndex, NewIntegerValue(int64(t.RowIDSeq)))
	}

	if len(row.Values) > len(t.Schema.Columns) {
		return -1, fmt.Errorf("row column count %d exceeds schema %d",
			len(row.Values), len(t.Schema.Columns))
	}

	for i, col := range t.Schema.Columns {
		if i >= len(row.Values) {
			continue
		}

		val := row.Values[i]
		if val.Type() != col.Type && val.Type() != TypeNull {
			return -1, fmt.Errorf("type mismatch for column %s: expected %s, got %s",
				col.Name, col.Type, val.Type())
		}

		if col.NotNull && val.Type() == TypeNull {
			// Allow null for PK only if it gets auto-incremented. We already handled it.
			if !col.PrimaryKey {
				return -1, fmt.Errorf("column %s cannot be null", col.Name)
			}
		}

		if col.PrimaryKey && val.Type() != TypeNull {
			colIndex := t.Schema.ColumnIndex(col.Name)
			for _, existingRow := range t.Rows {
				existingVal, _ := existingRow.Get(colIndex)
				if val.Equals(existingVal) {
					// If we just assigned this, we need to advance the sequence past any manually inserted higher value
					if intVal, ok := val.(*IntegerValue); ok {
						if intVal.Value >= int64(t.RowIDSeq) {
							t.RowIDSeq = int(intVal.Value)
						}
					}
					return -1, fmt.Errorf("primary key violation: duplicate value %s", val.ToString())
				}
			}
		}

		if col.Unique && val.Type() != TypeNull {
			colIndex := t.Schema.ColumnIndex(col.Name)
			for _, existingRow := range t.Rows {
				existingVal, _ := existingRow.Get(colIndex)
				if val.Equals(existingVal) {
					return -1, fmt.Errorf("unique constraint violation: duplicate value %s", val.ToString())
				}
			}
		}
	}

	for _, fk := range t.ForeignKeys {
		if err := t.checkForeignKey(row, fk); err != nil {
			return -1, fmt.Errorf("foreign key constraint violation: %w", err)
		}
	}

	finalRow := row
	if len(row.Values) < len(t.Schema.Columns) {
		newValues := make([]Value, len(t.Schema.Columns))
		copy(newValues, row.Values)

		for i := len(row.Values); i < len(t.Schema.Columns); i++ {
			col := t.Schema.Columns[i]
			if col.Default != nil {
				newValues[i] = col.Default.Clone()
			} else {
				// If this is a PK that was auto-incremented, it's already set.
				// Otherwise, it should be null.
				if i != pkColIndex {
					newValues[i] = NullValue{}
				}
			}
		}

		finalRow = NewRow(newValues)
	}

	// If a high PK value was inserted, adjust the sequence
	if pkColIndex != -1 {
		if pkVal, err := finalRow.Get(pkColIndex); err == nil {
			if intVal, ok := pkVal.(*IntegerValue); ok {
				if intVal.Value >= int64(t.RowIDSeq) {
					t.RowIDSeq = int(intVal.Value)
				}
			}
		}
	}
	
	rowIDToReturn := t.RowIDSeq - 1
	if isPKNull {
		rowIDToReturn = t.RowIDSeq
	}


	t.Rows = append(t.Rows, finalRow)
	t.RowIDSeq++


	for colName, index := range t.Indexes {
		colIndex := t.Schema.ColumnIndex(colName)
		if val, err := finalRow.Get(colIndex); err == nil && val.Type() != TypeNull {
			if err := index.Insert(val, rowIDToReturn); err != nil {
				t.Rows = t.Rows[:len(t.Rows)-1]
				t.RowIDSeq--
				return -1, fmt.Errorf("failed to update index: %w", err)
			}
		}
	}

	return rowIDToReturn, nil
}

func (t *Table) checkForeignKey(row *Row, fk *ForeignKey) error {
	fkValues := make([]Value, len(fk.Columns))
	for i, colName := range fk.Columns {
		colIndex := t.Schema.ColumnIndex(colName)
		val, err := row.Get(colIndex)
		if err != nil {
			return err
		}
		fkValues[i] = val
	}

	return nil
}

func (t *Table) Select(predicate func(*Row) bool) []*Row {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*Row, 0)
	for _, row := range t.Rows {
		if predicate == nil || predicate(row) {
			result = append(result, row.Clone())
		}
	}
	return result
}

func (t *Table) Update(predicate func(*Row) bool, updater func(*Row)) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	updated := 0
	for i, row := range t.Rows {
		if predicate == nil || predicate(row) {
			oldRow := row.Clone()
			updater(row)

			for _, col := range t.Schema.Columns {
				if col.PrimaryKey {
					colIndex := t.Schema.ColumnIndex(col.Name)
					newVal, _ := row.Get(colIndex)
					oldVal, _ := oldRow.Get(colIndex)

					if !newVal.Equals(oldVal) {
						return -1, fmt.Errorf("cannot update primary key column %s", col.Name)
					}
				}
			}

			for _, col := range t.Schema.Columns {
				if col.Unique {
					colIndex := t.Schema.ColumnIndex(col.Name)
					newVal, _ := row.Get(colIndex)
					oldVal, _ := oldRow.Get(colIndex)

					if !newVal.Equals(oldVal) {
						for j, otherRow := range t.Rows {
							if j != i {
								otherVal, _ := otherRow.Get(colIndex)
								if newVal.Equals(otherVal) {
									return -1, fmt.Errorf("unique constraint violation: duplicate value %s",
										newVal.ToString())
								}
							}
						}
					}
				}
			}

			updated++
		}
	}
	return updated, nil
}

func (t *Table) Delete(predicate func(*Row) bool) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	deleted := 0
	newRows := make([]*Row, 0)

	for _, row := range t.Rows {
		if predicate == nil || predicate(row) {
			deleted++
			for colName, index := range t.Indexes {
				colIndex := t.Schema.ColumnIndex(colName)
				if val, err := row.Get(colIndex); err == nil {
					index.Delete(val)
				}
			}
		} else {
			newRows = append(newRows, row)
		}
	}

	t.Rows = newRows
	return deleted, nil
}

func (t *Table) GetRow(rowID int) (*Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if rowID < 0 || rowID >= len(t.Rows) {
		return nil, fmt.Errorf("row not found: %d", rowID)
	}
	return t.Rows[rowID].Clone(), nil
}

func (t *Table) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Rows)
}

func (t *Table) Truncate() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Rows = make([]*Row, 0)
	t.RowIDSeq = 1

	for colName := range t.Indexes {
		t.Indexes[colName] = NewIndex()
	}
}

func (t *Table) AddForeignKey(fk *ForeignKey) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(fk.Columns) != len(fk.RefColumns) {
		return fmt.Errorf("foreign key column count mismatch")
	}

	for _, colName := range fk.Columns {
		if _, exists := t.Schema.GetColumn(colName); !exists {
			return fmt.Errorf("column %s not found", colName)
		}
	}

	t.ForeignKeys = append(t.ForeignKeys, fk)
	return nil
}

func (t *Table) GetForeignKeys() []*ForeignKey {
	t.mu.RLock()
	defer t.mu.RUnlock()

	fks := make([]*ForeignKey, len(t.ForeignKeys))
	copy(fks, t.ForeignKeys)
	return fks
}

func (t *Table) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := fmt.Sprintf("Table: %s\n", t.Name)
	result += fmt.Sprintf("Schema: %s\n", t.Schema.String())
	result += fmt.Sprintf("Rows: %d\n", len(t.Rows))
	result += fmt.Sprintf("Indexes: %d\n", len(t.Indexes))
	for colName := range t.Indexes {
		result += fmt.Sprintf("  - %s\n", colName)
	}
	return result
}
