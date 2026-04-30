package dbfly

import (
	"context"
	"strings"
)

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

type DatabaseMetaData interface {
	Dbms() string
	// DataType 数据类型转换
	DataType(string) string
	// GetTables 查找所有表
	GetTables(context.Context, Driver) ([]*Table, error)
	// GetColumns 查找指定表的所有列
	GetColumns(context.Context, Driver, string) ([]string, error)
	// GetIndexes 查找指定表的所有索引
	GetIndexes(context.Context, Driver, string) ([]*Index, error)
	// GetPrimaryKeys 查找指定表的主键
	GetPrimaryKeys(context.Context, Driver, string) ([]*PrimaryKey, error)
	// ExistsTable 判断是否存在指定表，返回实际表名
	ExistsTable(context.Context, Driver, string) (bool, string, error)
	// ExistsColumn 判断指定表中是否存在指定列，返回实际表名和列名
	ExistsColumn(context.Context, Driver, string, string) (bool, string, string, error)
	// ExistsIndex 判断指定表中是否存在指定索引，返回实际表名和索引名
	ExistsIndex(context.Context, Driver, string, string) (bool, string, string, error)
	// ExistsPrimaryKey 判断指定表中是否存在指定主键，返回实际表名
	ExistsPrimaryKey(context.Context, Driver, string) (bool, string, error)
	// Quoter 使用引号包裹器
	Quoter() *Quoter
}

type Table struct {
	Name      string
	TableType string
}

type Index struct {
	Name       string
	ColumnName string
}

type PrimaryKey struct {
	Name       string
	ColumnName string
}

type TableGetter func(context.Context, Driver) ([]*Table, error)
type ColumnGetter func(context.Context, Driver, string) ([]string, error)
type IndexGetter func(context.Context, Driver, string) ([]*Index, error)
type PrimaryKeyGetter func(context.Context, Driver, string) ([]*PrimaryKey, error)

func ExistsTable(getter TableGetter, ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	list, err := getter(ctx, driver)
	if err != nil {
		return false, "", err
	}
	tableName = strings.ToUpper(tableName)
	for _, table := range list {
		if strings.ToUpper(table.Name) == tableName {
			return true, table.Name, nil
		}
	}
	return false, "", nil
}

func ExistsColumn(tableGetter TableGetter, columnGetter ColumnGetter, ctx context.Context, driver Driver, tableName, columnName string) (bool, string, string, error) {
	var err error
	_, actualTableName, err := ExistsTable(tableGetter, ctx, driver, tableName)
	if err != nil {
		return false, "", "", err
	}
	columns, err := columnGetter(ctx, driver, actualTableName)
	if err != nil {
		return false, "", "", err
	}
	columnName = strings.ToUpper(columnName)
	for _, column := range columns {
		if strings.ToUpper(column) == columnName {
			return true, actualTableName, column, nil
		}
	}
	return false, "", "", nil
}

func ExistsIndex(tableGetter TableGetter, indexGetter IndexGetter, ctx context.Context, driver Driver, tableName, indexName string) (bool, string, string, error) {
	var err error
	_, actualTableName, err := ExistsTable(tableGetter, ctx, driver, tableName)
	if err != nil {
		return false, "", "", err
	}
	indexes, err := indexGetter(ctx, driver, actualTableName)
	if err != nil {
		return false, "", "", err
	}
	indexName = strings.ToUpper(indexName)
	for _, index := range indexes {
		if strings.ToUpper(index.Name) == indexName {
			return true, actualTableName, index.Name, nil
		}
	}
	return false, "", "", nil
}

func ExistsPrimaryKey(tableGetter TableGetter, primaryKeysGetter PrimaryKeyGetter, ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	var err error
	_, actualTableName, err := ExistsTable(tableGetter, ctx, driver, tableName)
	if err != nil {
		return false, "", err
	}
	primaryKeys, err := primaryKeysGetter(ctx, driver, actualTableName)
	if err != nil {
		return false, "", err
	}
	return len(primaryKeys) > 0, actualTableName, nil
}
