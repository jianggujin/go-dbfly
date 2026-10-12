package dbfly

import (
	"context"
	"database/sql"
	"io"
)

// Tx 事务接口
type Tx interface {
	// Execute 在事务中执行SQL
	Execute(context.Context, string, ...interface{}) (sql.Result, error)
	// Query 在事务中查询
	Query(context.Context, string, ...interface{}) (Rows, error)
	// Commit 提交事务
	Commit() error
	// Rollback 回滚事务
	Rollback() error
}

// Driver 不同框架对数据库操作的驱动接口
type Driver interface {
	// Execute 执行SQL
	Execute(context.Context, string, ...interface{}) (sql.Result, error)
	// Query 查询
	Query(context.Context, string, ...interface{}) (Rows, error)
	// BeginTx 开始事务
	BeginTx(context.Context) (Tx, error)
}

// Rows 查询结果
type Rows interface {
	io.Closer
	Columns() ([]string, error)
	Next() bool
	Scan(...interface{}) error
	Err() error
}

// SqlDriver 原生sql驱动实现
type SqlDriver struct {
	db     *sql.DB
	binder Binder
}

func NewSqlDriver(db *sql.DB, binder Binder) *SqlDriver {
	if binder == nil {
		binder = Question
	}
	return &SqlDriver{db: db, binder: binder}
}

func (d *SqlDriver) Execute(ctx context.Context, sql string, values ...interface{}) (sql.Result, error) {
	if len(values) > 0 {
		sql = d.binder.Rebind(sql)
	}
	return d.db.ExecContext(ctx, sql, values...)
}

func (d *SqlDriver) Query(ctx context.Context, sql string, values ...interface{}) (Rows, error) {
	if len(values) > 0 {
		sql = d.binder.Rebind(sql)
	}
	return d.db.QueryContext(ctx, sql, values...)
}

func (d *SqlDriver) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx, binder: d.binder}, nil
}

// sqlTx 基于原生sql.Tx的事务实现
type sqlTx struct {
	tx     *sql.Tx
	binder Binder
}

func (t *sqlTx) Execute(ctx context.Context, sql string, values ...interface{}) (sql.Result, error) {
	if len(values) > 0 {
		sql = t.binder.Rebind(sql)
	}
	return t.tx.ExecContext(ctx, sql, values...)
}

func (t *sqlTx) Query(ctx context.Context, sql string, values ...interface{}) (Rows, error) {
	if len(values) > 0 {
		sql = t.binder.Rebind(sql)
	}
	return t.tx.QueryContext(ctx, sql, values...)
}

func (t *sqlTx) Commit() error {
	return t.tx.Commit()
}

func (t *sqlTx) Rollback() error {
	return t.tx.Rollback()
}
