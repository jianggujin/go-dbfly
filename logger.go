package dbfly

// Logger 日志接口，用于记录迁移过程中的日志信息
type Logger interface {
	// Debug 记录调试级别日志
	Debug(string, ...any)
	// Info 记录信息级别日志
	Info(string, ...any)
	// Warn 记录警告级别日志
	Warn(string, ...any)
	// Error 记录错误级别日志
	Error(string, ...any)
}

// nopLogger 空日志实现，不输出任何内容
// 用作默认日志器，提供静默模式
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// LogSQLMode SQL 日志模式，使用位运算组合
type LogSQLMode int

const (
	LogSQLNone LogSQLMode = 0

	// LogSQLTemplate 记录 SQL 模板（带 ? 占位符）
	LogSQLTemplate LogSQLMode = 1 << iota
	// LogSQLParams 记录 SQL 参数
	LogSQLParams
)
