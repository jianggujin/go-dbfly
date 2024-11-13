package dbfly

import (
	"context"
	"database/sql"
	"fmt"
	"io"
)

// 不同框架对数据库操作的驱动接口
type Driver interface {
	// 执行SQL
	Execute(context.Context, string, ...interface{}) error
	// 查询
	Query(context.Context, string, ...interface{}) (Rows, error)
}

// 查询结果
type Rows interface {
	io.Closer
	Next() bool
	Scan(...interface{}) error
}

// 原生sql驱动实现
type SqlDriver struct {
	db *sql.DB
}

func NewSqlDriver(db *sql.DB) *SqlDriver {
	return &SqlDriver{db: db}
}

func (m *SqlDriver) Execute(ctx context.Context, sql string, values ...interface{}) error {
	_, err := m.db.ExecContext(ctx, sql, values...)
	return err
}

func (m *SqlDriver) Query(ctx context.Context, sql string, values ...interface{}) (Rows, error) {
	return m.db.QueryContext(ctx, sql, values...)
}

type DryRunDriver struct {
	count int
}

func NewDryRunDriver() *DryRunDriver {
	return &DryRunDriver{}
}

func (m *DryRunDriver) Execute(ctx context.Context, sql string, values ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		m.count++
		fmt.Printf("Execute %d\n", m.count)
		fmt.Printf("  --->SQL: %s\n", sql)
		fmt.Printf("  --->Values: %v\n", values)
		return nil
	}
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

func (m *DryRunDriver) Query(ctx context.Context, sql string, values ...interface{}) (Rows, error) {
	select {
	case <-ctx.Done():
		return &DryRunRows{}, ctx.Err()
	default:
		m.count++
		fmt.Printf("Query %d\n", m.count)
		fmt.Printf("  --->SQL: %s\n", sql)
		fmt.Printf("  --->Values: %v\n", values)
		return &DryRunRows{}, nil
	}
}
