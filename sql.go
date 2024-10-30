package dbfly

import (
	"database/sql"
	"fmt"
)

// 原生sql驱动实现
type SqlDriver struct {
	db *sql.DB
}

func NewSqlDriver(db *sql.DB) *SqlDriver {
	return &SqlDriver{db: db}
}

func (m *SqlDriver) Execute(sql string, values ...interface{}) error {
	_, err := m.db.Exec(sql, values...)
	return err
}

func (m *SqlDriver) Query(sql string, values ...interface{}) (Rows, error) {
	return m.db.Query(sql, values...)
}

type DryRunDriver struct {
	count int
}

func NewDryRunDriver() *DryRunDriver {
	return &DryRunDriver{}
}

func (m *DryRunDriver) Execute(sql string, values ...interface{}) error {
	m.count++
	fmt.Printf("Execute %d\n", m.count)
	fmt.Printf("  --->SQL: %s\n", sql)
	fmt.Printf("  --->Values: %v\n", values)
	return nil
}

type DryRunRows struct {
	row *sql.Rows
}

func (r *DryRunRows) Close() error {
	return nil
}

func (r *DryRunRows) Next() bool {
	return false
}

func (r *DryRunRows) Scan(...interface{}) error {
	return sql.ErrNoRows
}

func (m *DryRunDriver) Query(sql string, values ...interface{}) (Rows, error) {
	m.count++
	fmt.Printf("Query %d\n", m.count)
	fmt.Printf("  --->SQL: %s\n", sql)
	fmt.Printf("  --->Values: %v\n", values)
	return &DryRunRows{}, nil
}
