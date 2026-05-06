package dbfly

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

const defaultChangeLockTableName = "DBFLY_CHANGE_LOCK"

const (
	defaultLockRetryInterval = 100 * time.Millisecond
	defaultLockTimeout       = 30 * time.Second
	defaultLockMaxRetries    = 300 // 30s / 100ms
)

const (
	LOCK_COLUMN_ID        = "ID"
	LOCK_COLUMN_LOCKED    = "IS_LOCKED"
	LOCK_COLUMN_LOCKED_BY = "LOCKED_BY"
	LOCK_COLUMN_LOCK_TIME = "LOCK_TIME"
	LOCK_COLUMN_VERSION   = "VERSION"
)

type Unlock func(context.Context, *Dbfly) error

type Locker interface {
	Lock(context.Context, *Dbfly) (Unlock, error)
}

type DbLocker struct {
	tableName     string
	retryInterval time.Duration
	timeout       time.Duration
	maxRetries    int
}

type LockerOption func(*DbLocker)

func WithLockerTableName(tableName string) LockerOption {
	return func(r *DbLocker) {
		if tableName != "" {
			r.tableName = tableName
		}
	}
}

func WithLockRetryInterval(d time.Duration) LockerOption {
	return func(l *DbLocker) {
		if d > 0 {
			l.retryInterval = d
		}
	}
}

func WithLockTimeout(d time.Duration) LockerOption {
	return func(l *DbLocker) {
		if d > 0 {
			l.timeout = d
		}
	}
}

func WithLockMaxRetries(n int) LockerOption {
	return func(l *DbLocker) {
		if n > 0 {
			l.maxRetries = n
		}
	}
}

func NewDbLocker(opts ...LockerOption) *DbLocker {
	l := &DbLocker{
		tableName:     defaultChangeLockTableName,
		retryInterval: defaultLockRetryInterval,
		timeout:       defaultLockTimeout,
		maxRetries:    defaultLockMaxRetries,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *DbLocker) Lock(ctx context.Context, fly *Dbfly) (Unlock, error) {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()

	// 1. 确保锁表存在
	if err := l.createLockTable(ctx, fly); err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	quoter := metaData.Quoter()

	// 计算截止时间
	deadline := time.Now().Add(l.timeout)

	// 重试循环
	for retry := 0; retry < l.maxRetries; retry++ {
		// 检查超时
		if time.Now().After(deadline) {
			fly.logger.Error("lock acquisition timeout", "timeout", l.timeout)
			return nil, New("lock acquisition timeout after %v", l.timeout)
		}

		// 2. 乐观锁：查询当前版本
		currentVersion := -1
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = 1",
			quoter.MustQuote(LOCK_COLUMN_VERSION),
			quoter.MustQuote(l.tableName),
			quoter.MustQuote(LOCK_COLUMN_ID))

		if currentVersion, err = doGetScalar[int](ctx, driver, sql); err != nil && !errors.Is(err, NoData) {
			return nil, Wrap(err, "get current database schema failed")
		}

		if currentVersion > -1 {
			newVersion := currentVersion + 1

			// 3. UPDATE 带版本号条件（原子判断）
			updateSQL := fmt.Sprintf("UPDATE %s SET %s = 1, %s = ?, %s = ?, %s = ? WHERE %s = 1 AND %s = ?",
				quoter.MustQuote(l.tableName),
				quoter.MustQuote(LOCK_COLUMN_LOCKED),
				quoter.MustQuote(LOCK_COLUMN_LOCKED_BY),
				quoter.MustQuote(LOCK_COLUMN_LOCK_TIME),
				quoter.MustQuote(LOCK_COLUMN_VERSION),
				quoter.MustQuote(LOCK_COLUMN_ID),
				quoter.MustQuote(LOCK_COLUMN_VERSION))
			if err = driver.Execute(ctx, updateSQL, hostname, time.Now(), newVersion, currentVersion); err != nil {
				// UPDATE 失败（版本不匹配），等待后重试
				fly.logger.Warn("lock acquisition retry", "attempt", retry+1, "maxRetries", l.maxRetries)
				time.Sleep(l.retryInterval)
				continue
			}
			currentVersion = newVersion
		} else {
			currentVersion = 1
			// 4. INSERT 新记录
			insertSQL := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s, %s) VALUES(1, 1, ?, ?, ?)",
				quoter.MustQuote(l.tableName),
				quoter.MustQuote(LOCK_COLUMN_ID),
				quoter.MustQuote(LOCK_COLUMN_LOCKED),
				quoter.MustQuote(LOCK_COLUMN_LOCKED_BY),
				quoter.MustQuote(LOCK_COLUMN_LOCK_TIME),
				quoter.MustQuote(LOCK_COLUMN_VERSION))
			if err = driver.Execute(ctx, insertSQL, hostname, time.Now(), currentVersion); err != nil {
				fly.logger.Warn("lock acquisition retry", "attempt", retry+1, "maxRetries", l.maxRetries)
				// INSERT 失败（记录已存在），等待后重试
				time.Sleep(l.retryInterval)
				continue
			}
		}

		// 5. 复查验证（消除 TOCTOU）
		var lockedBy string
		var version int
		if lockedBy, version, err = l.getLockInfo(ctx, fly); err != nil {
			return nil, err
		}
		if lockedBy != hostname || currentVersion != version {
			fly.logger.Warn("lock acquisition retry", "attempt", retry+1, "maxRetries", l.maxRetries)
			// 验证失败，等待后重试
			time.Sleep(l.retryInterval)
			continue
		}

		unlockFunc := func(ctx context.Context, fly *Dbfly) error {
			return l.Unlock(ctx, fly, hostname, version)
		}

		return unlockFunc, nil
	}

	return nil, New("lock acquisition failed after %d retries", l.maxRetries)
}

func (l *DbLocker) createLockTable(ctx context.Context, fly *Dbfly) error {
	metaData := fly.Migratory().MetaData()
	driver := fly.Driver()

	// 检查表是否存在，如果不存在则创建
	exists, _, err := metaData.ExistsTable(ctx, driver, l.tableName)
	if err != nil {
		return Wrap(err, "check lock table exists failed")
	}
	if exists {
		return nil
	}

	quoter := metaData.Quoter()
	sql := fmt.Sprintf("CREATE TABLE %s(%s %s PRIMARY KEY, %s %s DEFAULT 0 NOT NULL, %s %s(255), %s %s, %s %s DEFAULT 1 NOT NULL)",
		quoter.MustQuote(l.tableName),
		quoter.MustQuote(LOCK_COLUMN_ID), metaData.DataType(Int),
		quoter.MustQuote(LOCK_COLUMN_LOCKED), metaData.DataType(Tinyint),
		quoter.MustQuote(LOCK_COLUMN_LOCKED_BY), metaData.DataType(Varchar),
		quoter.MustQuote(LOCK_COLUMN_LOCK_TIME), metaData.DataType(Timestamp),
		quoter.MustQuote(LOCK_COLUMN_VERSION), metaData.DataType(Int))

	if err = driver.Execute(ctx, sql); err != nil {
		// 创建失败，可能是并发已创建，再次检查
		var reErr error
		exists, _, reErr = metaData.ExistsTable(ctx, driver, l.tableName)
		if reErr != nil {
			return Wrap(reErr, "re-check lock table exists failed")
		}
		if exists {
			return nil // 表已存在，忽略创建错误
		}
		return Wrap(err, "create lock table failed")
	}
	return nil
}

// getLockInfo 查询当前锁定者和版本号
func (l *DbLocker) getLockInfo(ctx context.Context, fly *Dbfly) (string, int, error) {
	quoter := fly.Migratory().MetaData().Quoter()

	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = 1 AND %s = 1",
		quoter.MustQuote(LOCK_COLUMN_LOCKED_BY),
		quoter.MustQuote(LOCK_COLUMN_VERSION),
		quoter.MustQuote(l.tableName),
		quoter.MustQuote(LOCK_COLUMN_ID),
		quoter.MustQuote(LOCK_COLUMN_LOCKED))
	var lockedBy string
	var version int
	rows, err := fly.Driver().Query(ctx, sql)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Scan(&lockedBy, &version); err != nil {
			return "", 0, err
		}
		return lockedBy, version, nil
	}
	return "", 0, New("lock record not found")
}

func (l *DbLocker) Unlock(ctx context.Context, fly *Dbfly, hostname string, version int) error {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()
	quoter := metaData.Quoter()

	sql := fmt.Sprintf("UPDATE %s SET %s = 0 WHERE %s = 1 AND %s = ? AND %s = ?",
		quoter.MustQuote(l.tableName),
		quoter.MustQuote(LOCK_COLUMN_LOCKED),
		quoter.MustQuote(LOCK_COLUMN_ID),
		quoter.MustQuote(LOCK_COLUMN_LOCKED_BY),
		quoter.MustQuote(LOCK_COLUMN_VERSION))
	return driver.Execute(ctx, sql, hostname, version)
}
