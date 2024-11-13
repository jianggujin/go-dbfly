package dbfly

import (
	"context"
	"github.com/hashicorp/go-version"
	"sort"
	"time"
)

// 默认用于记录版本变化的表名
const changeTableName = "dbfly_change_log"
const allDialect = "$all"
const (
	Varchar   = "VARCHAR"
	Char      = "CHAR"
	Text      = "TEXT"
	Clob      = "CLOB"
	Boolean   = "BOOLEAN"
	Tinyint   = "TINYINT"
	Smallint  = "SMALLINT"
	Int       = "INT"
	Bigint    = "BIGINT"
	Decimal   = "DECIMAL"
	Date      = "DATE"
	Time      = "TIME"
	Timestamp = "TIMESTAMP"
	Blob      = "BLOB"
)

// 节点接口
type Node interface {
}

// 创建表节点
type CreateTableNode struct {
	TableName string        `xml:"tableName,attr"`
	Remarks   string        `xml:"remarks,attr"`
	Columns   []*ColumnNode `xml:"column"`
}

// 列节点
type ColumnNode struct {
	ColumnName         string               `xml:"columnName,attr"`
	DataType           string               `xml:"dataType,attr"`
	MaxLength          int                  `xml:"maxLength,attr"`
	NumericScale       int                  `xml:"numericScale,attr"`
	Nullable           bool                 `xml:"nullable,attr"`
	Unique             bool                 `xml:"unique,attr"`
	PrimaryKey         bool                 `xml:"primaryKey,attr"`
	DefaultValue       string               `xml:"defaultValue,attr"`
	DefaultOriginValue string               `xml:"defaultOriginValue,attr"`
	Remarks            string               `xml:"remarks,attr"`
	Dialects           []*ColumnDialectNode `xml:"columnDialect"`
}

// 列方言节点
type ColumnDialectNode struct {
	Dialect            string `xml:"dialect,attr"`
	DataType           string `xml:"dataType,attr"`
	DefaultValue       string `xml:"defaultValue,attr"`
	DefaultOriginValue string `xml:"defaultOriginValue,attr"`
}

// 创建索引节点
type CreateIndexNode struct {
	TableName string             `xml:"tableName,attr"`
	IndexName string             `xml:"indexName,attr"`
	Unique    bool               `xml:"unique,attr"`
	Columns   []*IndexColumnNode `xml:"indexColumn"`
}

// 索引列节点
type IndexColumnNode struct {
	ColumnName string `xml:"columnName,attr"`
}

// 创建主键节点
type CreatePrimaryKeyNode struct {
	TableName string           `xml:"tableName,attr"`
	KeyName   string           `xml:"keyName,attr"`
	Column    *IndexColumnNode `xml:"indexColumn"`
}

// 删除表节点
type DropTableNode struct {
	TableName string `xml:"tableName,attr"`
}

// 删除索引节点
type DropIndexNode struct {
	TableName string `xml:"tableName,attr"`
	IndexName string `xml:"indexName,attr"`
}

// 添加列节点
type AddColumnNode struct {
	TableName string        `xml:"tableName,attr"`
	Columns   []*ColumnNode `xml:"column"`
}

// 修改列节点
type AlterColumnNode struct {
	TableName  string      `xml:"tableName,attr"`
	ColumnName string      `xml:"columnName,attr"`
	Column     *ColumnNode `xml:"column"`
}

// 删除列节点
type DropColumnNode struct {
	TableName  string `xml:"tableName,attr"`
	ColumnName string `xml:"columnName,attr"`
}

// 删除主键节点
type DropPrimaryKeyNode struct {
	TableName string `xml:"tableName,attr"`
}

// 重命名表节点
type RenameTableNode struct {
	TableName    string `xml:"tableName,attr"`
	NewTableName string `xml:"newTableName,attr"`
}

// 重命名表说明节点
type AlterTableRemarksNode struct {
	TableName string `xml:"tableName,attr"`
	Remarks   string `xml:"remarks,attr"`
}

// SQL脚本节点
type ScriptNode struct {
	Dialect string `xml:"dialect,attr"`
	Value   string `xml:",chardata"`
}

// SQL版本合并接口
type Migratory interface {
	// 合并器名称
	Name() string
	// 初始化记录变更记录表
	InitChangeLogTable(context.Context, Driver, string) error
	// 最后一次版本信息
	LastVersion(context.Context, Driver, string) (*version.Version, error)
	// 创建一条新的表更记录
	NewChangeLog(context.Context, Driver, string, string) error
	// 完成一条表更记录
	CompleteChangeLog(context.Context, Driver, string, string) error
	// 创建表
	CreateTable(context.Context, Driver, *CreateTableNode) error
	// 创建索引
	CreateIndex(context.Context, Driver, *CreateIndexNode) error
	// 创建主键
	CreatePrimaryKey(context.Context, Driver, *CreatePrimaryKeyNode) error
	// 删除表
	DropTable(context.Context, Driver, *DropTableNode) error
	// 删除索引
	DropIndex(context.Context, Driver, *DropIndexNode) error
	// 添加列
	AddColumn(context.Context, Driver, *AddColumnNode) error
	// 修改列
	AlterColumn(context.Context, Driver, *AlterColumnNode) error
	// 删除列
	DropColumn(context.Context, Driver, *DropColumnNode) error
	// 删除主键
	DropPrimaryKey(context.Context, Driver, *DropPrimaryKeyNode) error
	// 重命名表
	RenameTable(context.Context, Driver, *RenameTableNode) error
	// 修改表说明
	AlterTableRemarks(context.Context, Driver, *AlterTableRemarksNode) error
	// 执行自定义SQL脚本
	Script(context.Context, Driver, *ScriptNode) error
}

type DefaultMigratory struct {
	name string
}

func (m *DefaultMigratory) Name() string {
	return m.name
}

func (m *DefaultMigratory) LastVersion(ctx context.Context, driver Driver, changeTableName string) (*version.Version, error) {
	rows, err := driver.Query(ctx, "SELECT change_version FROM "+changeTableName+" WHERE is_success = 1")
	if err != nil {
		return nil, err
	}
	var versions []*version.Version
	defer rows.Close()
	for rows.Next() {
		var changeVersion string
		if err := rows.Scan(&changeVersion); err != nil {
			return nil, err
		}
		ver, err := version.NewVersion(changeVersion)
		if err != nil {
			return nil, err
		}
		versions = append(versions, ver)
	}
	if len(versions) == 0 {
		return nil, nil
	}
	sort.Sort(version.Collection(versions))
	return versions[len(versions)-1], nil
}

func (m *DefaultMigratory) NewChangeLog(ctx context.Context, driver Driver, changeTableName, version string) error {
	return driver.Execute(ctx, "INSERT INTO "+changeTableName+"(change_version, is_success, created_at, updated_at) VALUES(?, 0, ?, ?)", version, time.Now(), time.Now())
}

func (m *DefaultMigratory) CompleteChangeLog(ctx context.Context, driver Driver, changeTableName, version string) error {
	return driver.Execute(ctx, "UPDATE "+changeTableName+" SET is_success = 1, updated_at = ? WHERE change_version = ? AND is_success = 0", time.Now(), version)
}

func (m *DefaultMigratory) Script(ctx context.Context, driver Driver, node *ScriptNode) error {
	if node.Value == "" || (node.Dialect != m.name && node.Dialect != allDialect) {
		return nil
	}
	for _, statement := range splitSQLStatements(node.Value) {
		if err := driver.Execute(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
