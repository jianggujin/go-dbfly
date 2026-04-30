package dbfly

import (
	"context"
	"fmt"
	"time"
)

const defaultChangeLogTableName = "DBFLY_CHANGE_LOG"

const (
	COLUMN_CHANGESET_ID   = "CHANGESET_ID"
	COLUMN_AUTHOR         = "AUTHOR"
	COLUMN_FILENAME       = "FILENAME"
	COLUMN_ORDER_EXECUTED = "ORDER_EXECUTED"
	COLUMN_IS_SUCCESS     = "IS_SUCCESS"
	COLUMN_CREATED_AT     = "CREATED_AT"
	COLUMN_UPDATED_AT     = "UPDATED_AT"
)

type Recorder interface {
	// InitChangeLogTable 初始化记录变更记录表
	InitChangeLogTable(context.Context, *Dbfly) error
	// GetExecutedChangeSets 获取已执行的变更集ID集合
	GetExecutedChangeSets(context.Context, *Dbfly) (map[string]bool, error)
	// NewChangeLog 创建一条新的变更记录
	NewChangeLog(context.Context, *Dbfly, string, string, string, int) error
	// CompleteChangeLog 完成一条变更记录
	CompleteChangeLog(context.Context, *Dbfly, string) error
}

type DbRecorder struct {
	tableName string
}

type RecorderOption func(*DbRecorder)

func WithRecorderTableName(tableName string) RecorderOption {
	return func(r *DbRecorder) {
		if tableName != "" {
			r.tableName = tableName
		}
	}
}

func NewDbRecorder(opts ...RecorderOption) *DbRecorder {
	r := &DbRecorder{tableName: defaultChangeLogTableName}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *DbRecorder) InitChangeLogTable(ctx context.Context, fly *Dbfly) error {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()

	// 检查表是否存在，如果不存在则创建
	exists, _, err := metaData.ExistsTable(ctx, driver, r.tableName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	quoter := metaData.Quoter()
	sql := fmt.Sprintf("CREATE TABLE %s(%s %s(255) PRIMARY KEY, %s %s(255), %s %s(255), %s %s NOT NULL, %s %s DEFAULT 0 NOT NULL, %s %s, %s %s)",
		quoter.MustQuote(r.tableName),
		quoter.MustQuote(COLUMN_CHANGESET_ID), metaData.DataType(Varchar),
		quoter.MustQuote(COLUMN_AUTHOR), metaData.DataType(Varchar),
		quoter.MustQuote(COLUMN_FILENAME), metaData.DataType(Varchar),
		quoter.MustQuote(COLUMN_ORDER_EXECUTED), metaData.DataType(Int),
		quoter.MustQuote(COLUMN_IS_SUCCESS), metaData.DataType(Tinyint),
		quoter.MustQuote(COLUMN_CREATED_AT), metaData.DataType(Timestamp),
		quoter.MustQuote(COLUMN_UPDATED_AT), metaData.DataType(Timestamp),
	)
	return driver.Execute(ctx, sql)
}

func (r *DbRecorder) GetExecutedChangeSets(ctx context.Context, fly *Dbfly) (map[string]bool, error) {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()
	quoter := metaData.Quoter()
	rows, err := driver.Query(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s = 1",
		quoter.MustQuote(COLUMN_CHANGESET_ID), quoter.MustQuote(r.tableName), quoter.MustQuote(COLUMN_IS_SUCCESS)))
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	defer rows.Close()
	for rows.Next() {
		var changeSetId string
		if err = rows.Scan(&changeSetId); err != nil {
			return nil, err
		}
		result[changeSetId] = true
	}
	return result, nil
}

func (r *DbRecorder) NewChangeLog(ctx context.Context, fly *Dbfly, changeSetId, author, filename string, orderExecuted int) error {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()
	quoter := metaData.Quoter()
	// 先删除失败的记录（IS_SUCCESS = 0），避免主键冲突
	if err := driver.Execute(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s = ? AND %s = 0",
			quoter.MustQuote(r.tableName),
			quoter.MustQuote(COLUMN_CHANGESET_ID),
			quoter.MustQuote(COLUMN_IS_SUCCESS)),
		changeSetId); err != nil {
		return err
	}
	return driver.Execute(ctx,
		fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s, %s, %s, %s) VALUES(?, ?, ?, ?, 0, ?, ?)",
			quoter.MustQuote(r.tableName),
			quoter.MustQuote(COLUMN_CHANGESET_ID), quoter.MustQuote(COLUMN_AUTHOR),
			quoter.MustQuote(COLUMN_FILENAME), quoter.MustQuote(COLUMN_ORDER_EXECUTED),
			quoter.MustQuote(COLUMN_IS_SUCCESS), quoter.MustQuote(COLUMN_CREATED_AT), quoter.MustQuote(COLUMN_UPDATED_AT)),
		changeSetId, author, filename, orderExecuted, time.Now(), time.Now())
}

func (r *DbRecorder) CompleteChangeLog(ctx context.Context, fly *Dbfly, changeSetId string) error {
	migratory := fly.Migratory()
	driver := fly.Driver()
	metaData := migratory.MetaData()
	quoter := metaData.Quoter()
	return driver.Execute(ctx,
		fmt.Sprintf("UPDATE %s SET %s = 1, %s = ? WHERE %s = ? AND %s = 0",
			quoter.MustQuote(r.tableName), quoter.MustQuote(COLUMN_IS_SUCCESS),
			quoter.MustQuote(COLUMN_UPDATED_AT), quoter.MustQuote(COLUMN_CHANGESET_ID), quoter.MustQuote(COLUMN_IS_SUCCESS)),
		time.Now(), changeSetId)
}
