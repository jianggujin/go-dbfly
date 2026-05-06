package dbfly

import (
	"context"
	"testing"
)

// mockLogger 记录日志调用用于断言
type mockLogger struct {
	debugs []logEntry
	infos  []logEntry
	warns  []logEntry
	errors []logEntry
}

type logEntry struct {
	msg           string
	keysAndValues []any
}

func (m *mockLogger) Debug(msg string, keysAndValues ...any) {
	m.debugs = append(m.debugs, logEntry{msg: msg, keysAndValues: keysAndValues})
}

func (m *mockLogger) Info(msg string, keysAndValues ...any) {
	m.infos = append(m.infos, logEntry{msg: msg, keysAndValues: keysAndValues})
}

func (m *mockLogger) Warn(msg string, keysAndValues ...any) {
	m.warns = append(m.warns, logEntry{msg: msg, keysAndValues: keysAndValues})
}

func (m *mockLogger) Error(msg string, keysAndValues ...any) {
	m.errors = append(m.errors, logEntry{msg: msg, keysAndValues: keysAndValues})
}

// mockDriver 记录 Execute/Query 调用
type mockDriver struct {
	executeSQL    string
	executeArgs   []any
	querySQL      string
	queryArgs     []any
	beginTxCalled bool
}

func (m *mockDriver) Execute(_ context.Context, sql string, args ...any) error {
	m.executeSQL = sql
	m.executeArgs = args
	return nil
}

func (m *mockDriver) Query(_ context.Context, sql string, args ...any) (Rows, error) {
	m.querySQL = sql
	m.queryArgs = args
	return &mockRows{}, nil
}

func (m *mockDriver) BeginTx(_ context.Context) (Tx, error) {
	m.beginTxCalled = true
	return &mockTx{}, nil
}

type mockRows struct{}

func (m *mockRows) Close() error               { return nil }
func (m *mockRows) Columns() ([]string, error) { return nil, nil }
func (m *mockRows) Next() bool                 { return false }
func (m *mockRows) Scan(...any) error          { return nil }
func (m *mockRows) Err() error                 { return nil }

type mockTx struct{}

func (m *mockTx) Execute(_ context.Context, sql string, args ...any) error { return nil }
func (m *mockTx) Query(_ context.Context, sql string, args ...any) (Rows, error) {
	return &mockRows{}, nil
}
func (m *mockTx) Commit() error   { return nil }
func (m *mockTx) Rollback() error { return nil }

func TestLoggingDriver_None(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLNone)

	_ = ld.Execute(context.Background(), "SELECT 1")
	if len(logger.debugs) != 0 {
		t.Errorf("LogSQLNone 不应记录日志，实际记录了 %d 条", len(logger.debugs))
	}
	if driver.executeSQL != "SELECT 1" {
		t.Errorf("应该委托到底层 driver，实际 SQL: %s", driver.executeSQL)
	}
}

func TestLoggingDriver_Template(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLTemplate)

	_ = ld.Execute(context.Background(), "SELECT ?", 42)

	if len(logger.debugs) != 1 {
		t.Fatalf("应该记录 1 条日志，实际记录了 %d 条", len(logger.debugs))
	}
	if logger.debugs[0].msg != "execute SQL" {
		t.Errorf("消息应该是 'execute SQL'，实际是 %q", logger.debugs[0].msg)
	}
	if driver.executeSQL != "SELECT ?" {
		t.Errorf("应该委托到底层 driver")
	}
}

func TestLoggingDriver_TemplateAndParams(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLTemplate|LogSQLParams)

	_ = ld.Execute(context.Background(), "SELECT ?", 42)

	if len(logger.debugs) != 2 {
		t.Fatalf("应该记录 2 条日志，实际记录了 %d 条", len(logger.debugs))
	}
	if logger.debugs[0].msg != "execute SQL" {
		t.Errorf("第一条消息应该是 'execute SQL'，实际是 %q", logger.debugs[0].msg)
	}
	if logger.debugs[1].msg != "SQL parameters" {
		t.Errorf("第二条消息应该是 'SQL parameters'，实际是 %q", logger.debugs[1].msg)
	}
}

func TestLoggingDriver_Query(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLTemplate)

	_, _ = ld.Query(context.Background(), "SELECT * FROM users")

	if len(logger.debugs) != 1 {
		t.Fatalf("应该记录 1 条日志，实际记录了 %d 条", len(logger.debugs))
	}
	if logger.debugs[0].msg != "query SQL" {
		t.Errorf("消息应该是 'query SQL'，实际是 %q", logger.debugs[0].msg)
	}
}

func TestLoggingDriver_ParamsOnly_NoArgs(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLParams)

	_ = ld.Execute(context.Background(), "SELECT 1")

	// LogSQLParams 但无参数时不应记录 SQL 参数
	if len(logger.debugs) != 0 {
		t.Errorf("无参数时不应记录 SQL 参数，实际记录了 %d 条", len(logger.debugs))
	}
}

func TestLoggingDriver_BeginTx_ReturnsLoggingTx(t *testing.T) {
	logger := &mockLogger{}
	driver := &mockDriver{}
	ld := NewLoggingDriver(driver, logger, LogSQLTemplate)

	tx, err := ld.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx 不应返回错误: %v", err)
	}

	loggingTx, ok := tx.(*LoggingTx)
	if !ok {
		t.Fatal("BeginTx 应该返回 *LoggingTx")
	}
	_ = loggingTx
}

func TestLoggingTx_Execute(t *testing.T) {
	logger := &mockLogger{}
	innerTx := &mockTx{}
	ltx := &LoggingTx{tx: innerTx, logger: logger, logSQLMode: LogSQLTemplate | LogSQLParams}

	_ = ltx.Execute(context.Background(), "INSERT INTO t VALUES (?)", 1)

	if len(logger.debugs) != 2 {
		t.Fatalf("应该记录 2 条日志，实际记录了 %d 条", len(logger.debugs))
	}
	if logger.debugs[0].msg != "execute SQL (tx)" {
		t.Errorf("消息应该是 'execute SQL (tx)'，实际是 %q", logger.debugs[0].msg)
	}
}

func TestLoggingTx_Commit_Rollback(t *testing.T) {
	logger := &mockLogger{}
	innerTx := &mockTx{}
	ltx := &LoggingTx{tx: innerTx, logger: logger, logSQLMode: LogSQLTemplate}

	if err := ltx.Commit(); err != nil {
		t.Errorf("Commit 不应返回错误: %v", err)
	}
	if err := ltx.Rollback(); err != nil {
		t.Errorf("Rollback 不应返回错误: %v", err)
	}
}
