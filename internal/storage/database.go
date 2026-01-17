package storage

import (
	"fmt"
	"sync"
)

type Database struct {
	tables map[string]*Table
	mu     sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		tables: make(map[string]*Table),
	}
}

func (db *Database) CreateTable(name string, schema *Schema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	table := NewTable(name, schema)

	for _, col := range schema.Columns {
		if col.PrimaryKey {
			if err := table.AddIndex(col.Name); err != nil {
				return fmt.Errorf("failed to create primary key index: %w", err)
			}
		} else if col.Unique {
			if err := table.AddIndex(col.Name); err != nil {
				return fmt.Errorf("failed to create unique index: %w", err)
			}
		}
	}

	db.tables[name] = table
	return nil
}

func (db *Database) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; !exists {
		return fmt.Errorf("table %s not found", name)
	}

	delete(db.tables, name)
	return nil
}

func (db *Database) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return table, nil
}

func (db *Database) TableExists(name string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, exists := db.tables[name]
	return exists
}

func (db *Database) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tables = append(tables, name)
	}
	return tables
}

func (db *Database) GetSchema(tableName string) (*Schema, error) {
	table, err := db.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.Schema, nil
}

func (db *Database) AddForeignKey(tableName string, fk *ForeignKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	refTable, exists := db.tables[fk.RefTable]
	if !exists {
		return fmt.Errorf("referenced table %s not found", fk.RefTable)
	}

	for _, refColName := range fk.RefColumns {
		if _, exists := refTable.Schema.GetColumn(refColName); !exists {
			return fmt.Errorf("referenced column %s not found in table %s", refColName, fk.RefTable)
		}
	}

	return table.AddForeignKey(fk)
}

func (db *Database) CascadeDelete(tableName string, rowID int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	pkCols := table.Schema.PrimaryKeyColumns()
	if len(pkCols) != 1 {
		return fmt.Errorf("cascade delete only supported for single-column primary keys")
	}

	pkCol := pkCols[0]
	row, err := table.GetRow(rowID)
	if err != nil {
		return err
	}

	pkValue, _ := row.Get(table.Schema.ColumnIndex(pkCol.Name))

	for otherTableName, otherTable := range db.tables {
		if otherTableName == tableName {
			continue
		}

		for _, fk := range otherTable.ForeignKeys {
			if fk.RefTable == tableName && fk.OnDelete == FKActionCascade {
				fkColIndex := otherTable.Schema.ColumnIndex(fk.Columns[0])
				for idx, otherRow := range otherTable.Rows {
					fkValue, _ := otherRow.Get(fkColIndex)
					if pkValue.Equals(fkValue) {
						db.cascadeDeleteInternal(otherTableName, idx)
					}
				}
			}
		}
	}

	return nil
}

func (db *Database) cascadeDeleteInternal(tableName string, rowID int) error {
	table := db.tables[tableName]
	row := table.Rows[rowID]

	for _, fk := range table.ForeignKeys {
		if fk.OnDelete == FKActionCascade {
			refTable := db.tables[fk.RefTable]
			pkCols := refTable.Schema.PrimaryKeyColumns()
			if len(pkCols) == 1 {
				pkCol := pkCols[0]
				pkValue, _ := row.Get(table.Schema.ColumnIndex(fk.Columns[0]))
				for idx, refRow := range refTable.Rows {
					refValue, _ := refRow.Get(refTable.Schema.ColumnIndex(pkCol.Name))
					if pkValue.Equals(refValue) {
						db.cascadeDeleteInternal(fk.RefTable, idx)
					}
				}
			}
		}
	}

	for colName, index := range table.Indexes {
		colIndex := table.Schema.ColumnIndex(colName)
		if val, err := row.Get(colIndex); err == nil {
			_ = index
			index.Delete(val)
		}
	}

	table.Rows = append(table.Rows[:rowID], table.Rows[rowID+1:]...)

	return nil
}

func (db *Database) String() string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := "Database:\n"
	for name, table := range db.tables {
		result += fmt.Sprintf("  %s: %d rows\n", name, table.Count())
	}
	return result
}
