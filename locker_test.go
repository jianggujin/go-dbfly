package dbfly

import (
	"testing"
	"time"
)

func TestNewDbLocker_Defaults(t *testing.T) {
	l := NewDbLocker()
	if l.tableName != defaultChangeLockTableName {
		t.Errorf("tableName = %s, want %s", l.tableName, defaultChangeLockTableName)
	}
	if l.retryInterval != defaultLockRetryInterval {
		t.Errorf("retryInterval = %v, want %v", l.retryInterval, defaultLockRetryInterval)
	}
	if l.timeout != defaultLockTimeout {
		t.Errorf("timeout = %v, want %v", l.timeout, defaultLockTimeout)
	}
	if l.maxRetries != defaultLockMaxRetries {
		t.Errorf("maxRetries = %d, want %d", l.maxRetries, defaultLockMaxRetries)
	}
}

func TestNewDbLocker_WithOptions(t *testing.T) {
	l := NewDbLocker(
		WithLockerTableName("custom_lock_table"),
		WithLockRetryInterval(200*time.Millisecond),
		WithLockTimeout(60*time.Second),
		WithLockMaxRetries(100),
	)
	if l.tableName != "custom_lock_table" {
		t.Errorf("tableName = %s, want custom_lock_table", l.tableName)
	}
	if l.retryInterval != 200*time.Millisecond {
		t.Errorf("retryInterval = %v, want 200ms", l.retryInterval)
	}
	if l.timeout != 60*time.Second {
		t.Errorf("timeout = %v, want 60s", l.timeout)
	}
	if l.maxRetries != 100 {
		t.Errorf("maxRetries = %d, want 100", l.maxRetries)
	}
}

func TestNewDbLocker_ZeroValuesIgnored(t *testing.T) {
	l := NewDbLocker(
		WithLockerTableName(""),
		WithLockRetryInterval(0),
		WithLockTimeout(0),
		WithLockMaxRetries(0),
	)
	// 零值应被忽略，保持默认值
	if l.tableName != defaultChangeLockTableName {
		t.Errorf("tableName = %s, want %s", l.tableName, defaultChangeLockTableName)
	}
	if l.retryInterval != defaultLockRetryInterval {
		t.Errorf("retryInterval = %v, want %v", l.retryInterval, defaultLockRetryInterval)
	}
	if l.timeout != defaultLockTimeout {
		t.Errorf("timeout = %v, want %v", l.timeout, defaultLockTimeout)
	}
	if l.maxRetries != defaultLockMaxRetries {
		t.Errorf("maxRetries = %d, want %d", l.maxRetries, defaultLockMaxRetries)
	}
}
