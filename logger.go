package dbfly

// Logger 日志接口，用于记录迁移过程中的日志信息
type Logger interface {
	// Debug 记录调试级别日志
	Debug(msg string, keysAndValues ...any)
	// Info 记录信息级别日志
	Info(msg string, keysAndValues ...any)
	// Warn 记录警告级别日志
	Warn(msg string, keysAndValues ...any)
	// Error 记录错误级别日志
	Error(msg string, keysAndValues ...any)
}

// nopLogger 空日志实现，不输出任何内容
// 用作默认日志器，提供静默模式
type nopLogger struct{}

func (nopLogger) Debug(msg string, keysAndValues ...any) {}
func (nopLogger) Info(msg string, keysAndValues ...any)  {}
func (nopLogger) Warn(msg string, keysAndValues ...any)  {}
func (nopLogger) Error(msg string, keysAndValues ...any) {}
