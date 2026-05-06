package dbfly

import "context"

// LoggingDriver 包装 Driver，统一处理 SQL 日志
type LoggingDriver struct {
	driver     Driver
	logger     Logger
	logSQLMode LogSQLMode
}

// NewLoggingDriver 创建带日志的 Driver 包装器
func NewLoggingDriver(driver Driver, logger Logger, logSQLMode LogSQLMode) *LoggingDriver {
	return &LoggingDriver{
		driver:     driver,
		logger:     logger,
		logSQLMode: logSQLMode,
	}
}

func (l *LoggingDriver) Execute(ctx context.Context, sql string, args ...any) error {
	l.logSQL("execute SQL", sql, args)
	return l.driver.Execute(ctx, sql, args...)
}

func (l *LoggingDriver) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	l.logSQL("query SQL", sql, args)
	return l.driver.Query(ctx, sql, args...)
}

func (l *LoggingDriver) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := l.driver.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &LoggingTx{tx: tx, logger: l.logger, logSQLMode: l.logSQLMode}, nil
}

func (l *LoggingDriver) logSQL(action, sql string, args []any) {
	if l.logSQLMode&LogSQLTemplate != 0 {
		l.logger.Debug(action, "sql", sql)
	}
	if l.logSQLMode&LogSQLParams != 0 && len(args) > 0 {
		l.logger.Debug("SQL parameters", "args", args)
	}
}

// LoggingTx 包装 Tx，确保事务内的 SQL 也被记录
type LoggingTx struct {
	tx         Tx
	logger     Logger
	logSQLMode LogSQLMode
}

func (l *LoggingTx) Execute(ctx context.Context, sql string, args ...any) error {
	l.logSQL("execute SQL (tx)", sql, args)
	return l.tx.Execute(ctx, sql, args...)
}

func (l *LoggingTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	l.logSQL("query SQL (tx)", sql, args)
	return l.tx.Query(ctx, sql, args...)
}

func (l *LoggingTx) Commit() error {
	return l.tx.Commit()
}

func (l *LoggingTx) Rollback() error {
	return l.tx.Rollback()
}

func (l *LoggingTx) logSQL(action, sql string, args []any) {
	if l.logSQLMode&LogSQLTemplate != 0 {
		l.logger.Debug(action, "sql", sql)
	}
	if l.logSQLMode&LogSQLParams != 0 && len(args) > 0 {
		l.logger.Debug("SQL parameters", "args", args)
	}
}
