package dbfly

import (
	"context"
)

type DamengDatabaseMetaData struct {
	quoter *Quoter
}

func NewDamengDatabaseMetaData() *DamengDatabaseMetaData {
	return &DamengDatabaseMetaData{
		quoter: NewQuoter('"', '"', AlwaysReserve),
	}
}

func (m *DamengDatabaseMetaData) Dbms() string {
	return "DM DBMS"
}

func (m *DamengDatabaseMetaData) DataType(str string) string {
	switch str {
	case Varchar:
		return "VARCHAR"
	case Char:
		return "CHAR"
	case Text:
		return "TEXT"
	case Clob:
		return "CLOB"
	case Boolean:
		return "BOOLEAN"
	case Tinyint:
		return "TINYINT"
	case Smallint:
		return "SMALLINT"
	case Int:
		return "INT"
	case Bigint:
		return "BIGINT"
	case Decimal:
		return "DECIMAL"
	case Date:
		return "DATE"
	case Time:
		return "TIME"
	case Timestamp:
		return "TIMESTAMP"
	case Blob:
		return "BLOB"
	}
	return str
}

func (m *DamengDatabaseMetaData) GetTables(ctx context.Context, driver Driver) ([]*Table, error) {
	sql := `SELECT o.object_name AS table_name,
       o.object_type AS table_type
FROM USER_OBJECTS o
WHERE o.object_type IN ('TABLE', 'VIEW')
ORDER BY table_type, table_name`
	return doGetSlices[Table](ctx, driver, func(rows Rows, t *Table) error {
		return rows.Scan(&t.Name, &t.TableType)
	}, sql)
}

func (m *DamengDatabaseMetaData) GetColumns(ctx context.Context, driver Driver, tableName string) ([]string, error) {
	sql := `SELECT t.column_name FROM USER_TAB_COLUMNS t WHERE t.table_name = ?`
	return doGetScalars[string](ctx, driver, sql, tableName)
}

func (m *DamengDatabaseMetaData) GetIndexes(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
	sql := `select i.index_name,
       c.column_name
from USER_INDEXES i,
     USER_IND_COLUMNS c
where i.table_name = ?
  and i.index_name = c.index_name
  and i.table_name = c.table_name
order by index_name`
	return doGetSlices[Index](ctx, driver, func(rows Rows, t *Index) error {
		return rows.Scan(&t.Name, &t.ColumnName)
	}, sql, tableName)
}

func (m *DamengDatabaseMetaData) GetPrimaryKeys(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
	sql := `SELECT c.constraint_name AS PK_NAME,
       cc.column_name    AS COLUMN_NAME
FROM USER_CONSTRAINTS c
         JOIN USER_CONS_COLUMNS cc
              ON c.constraint_name = cc.constraint_name
WHERE c.constraint_type = 'P'
  AND c.table_name = ?`
	return doGetSlices[PrimaryKey](ctx, driver, func(rows Rows, t *PrimaryKey) error {
		return rows.Scan(&t.Name, &t.ColumnName)
	}, sql, tableName)
}

func (m *DamengDatabaseMetaData) ExistsTable(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsTable(m.GetTables, ctx, driver, tableName)
}

func (m *DamengDatabaseMetaData) ExistsColumn(ctx context.Context, driver Driver, tableName, columnName string) (bool, string, string, error) {
	return ExistsColumn(m.GetTables, m.GetColumns, ctx, driver, tableName, columnName)
}

func (m *DamengDatabaseMetaData) ExistsIndex(ctx context.Context, driver Driver, tableName, indexName string) (bool, string, string, error) {
	return ExistsIndex(m.GetTables, m.GetIndexes, ctx, driver, tableName, indexName)
}

func (m *DamengDatabaseMetaData) ExistsPrimaryKey(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsPrimaryKey(m.GetTables, m.GetPrimaryKeys, ctx, driver, tableName)
}

func (m *DamengDatabaseMetaData) Quoter() *Quoter {
	return m.quoter
}

// DamengMigratory 达梦迁移实现
type DamengMigratory struct {
	DefaultMigratory
}

// NewDamengMigratory 创建一个达梦迁移实现实例
func NewDamengMigratory() Migratory {
	return &DamengMigratory{
		DefaultMigratory: NewDefaultMigratory("dm", NewDamengDatabaseMetaData()),
	}
}
