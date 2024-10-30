package dbfly

import (
	"github.com/hashicorp/go-version"
	"io"
)

// 默认用于记录版本变化的表名
const changeTableName = "dbfly_change_log"
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
	// 设置数据库版本变更记录表表名
	SetChangeTableName(string)
	// 最后一次版本信息
	LastVersion() (*version.Version, error)
	// 合并指定版本
	Migrate([]Node, *version.Version) error
}

// 不同框架对数据库操作的驱动接口
type Driver interface {
	// 执行SQL
	Execute(string, ...interface{}) error
	// 查询
	Query(string, ...interface{}) (Rows, error)
}

// 查询结果
type Rows interface {
	io.Closer
	Next() bool
	Scan(...interface{}) error
}
