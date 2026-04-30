package dbfly

import (
	"context"
)

// VastbaseDatabaseMetaData VastBase元数据实现
type VastbaseDatabaseMetaData struct {
	quoter *Quoter
	schema string
}

func NewVastbaseDatabaseMetaData() *VastbaseDatabaseMetaData {
	return &VastbaseDatabaseMetaData{
		quoter: NewQuoter('"', '"', AlwaysReserve),
	}
}

func (m *VastbaseDatabaseMetaData) Dbms() string {
	return "VastBase"
}

func (m *VastbaseDatabaseMetaData) DataType(str string) string {
	// VastBase 与 PostgreSQL 数据类型相同
	switch str {
	case Varchar:
		return "VARCHAR"
	case Char:
		return "CHAR"
	case Text:
		return "TEXT"
	case Clob:
		return "TEXT"
	case Boolean:
		return "SMALLINT"
	case Tinyint:
		return "SMALLINT"
	case Smallint:
		return "SMALLINT"
	case Int:
		return "INTEGER"
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
		return "BYTEA"
	}
	return str
}

func (m *VastbaseDatabaseMetaData) getSchema(ctx context.Context, driver Driver) (string, error) {
	if m.schema == "" {
		sql := "select current_schema()"
		schema, err := doGetScalar[string](ctx, driver, sql)
		if err != nil {
			return "", err
		}
		m.schema = schema
	}
	return m.schema, nil
}

func (m *VastbaseDatabaseMetaData) GetTables(ctx context.Context, driver Driver) ([]*Table, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `select tablename AS "TABLE_NAME", 'TABLE' AS "TABLE_TYPE" from pg_tables  WHERE schemaname = ?
union
select viewname AS "TABLE_NAME", 'VIEW' AS "TABLE_TYPE" from pg_views  WHERE schemaname = ?`
	return doGetSlices[Table](ctx, driver, func(rows Rows, t *Table) error {
		return rows.Scan(&t.Name, &t.TableType)
	}, sql, schema, schema)
}

func (m *VastbaseDatabaseMetaData) GetColumns(ctx context.Context, driver Driver, tableName string) ([]string, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SELECT a.attname
FROM pg_catalog.pg_namespace n
         JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
         JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
WHERE c.relkind in ('r', 'p', 'v', 'f', 'm')
  and a.attnum > 0
  AND NOT a.attisdropped
  AND n.nspname = ?
  AND c.relname = ?`
	return doGetScalars[string](ctx, driver, sql, schema, tableName)
}

func (m *VastbaseDatabaseMetaData) GetIndexes(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SELECT tmp.INDEX_NAME                                                                          AS "INDEX_NAME",
       trim(both '"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS "COLUMN_NAME"
FROM (SELECT ci.relname                                       AS INDEX_NAME,
             (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION,
             ci.oid                                           AS CI_OID
      FROM pg_catalog.pg_class ct
               JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
               JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)
               JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE true
        AND n.nspname = ?
        AND ct.relname = ?) AS tmp
ORDER BY "INDEX_NAME"`
	return doGetSlices[Index](ctx, driver, func(rows Rows, t *Index) error {
		return rows.Scan(&t.Name, &t.ColumnName)
	}, sql, schema, tableName)
}

func (m *VastbaseDatabaseMetaData) GetPrimaryKeys(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SELECT result.COLUMN_NAME AS "COLUMN_NAME",
       result.PK_NAME     AS "PK_NAME"
FROM (SELECT a.attname                                        AS COLUMN_NAME,
             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
             ci.relname                                       AS PK_NAME,
             information_schema._pg_expandarray(i.indkey)     AS KEYS,
             a.attnum                                         AS A_ATTNUM,
             i.indnkeyatts                                    as KEY_COUNT
      FROM pg_catalog.pg_class ct
               JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
               JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
               JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
               JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
      WHERE n.nspname = ?
        AND ct.relname = ?
        AND i.indisprimary) result
where result.A_ATTNUM = (result.KEYS).x
  AND result.KEY_SEQ <= KEY_COUNT`
	return doGetSlices[PrimaryKey](ctx, driver, func(rows Rows, t *PrimaryKey) error {
		return rows.Scan(&t.ColumnName, &t.Name)
	}, sql, schema, tableName)
}

func (m *VastbaseDatabaseMetaData) ExistsTable(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsTable(m.GetTables, ctx, driver, tableName)
}

func (m *VastbaseDatabaseMetaData) ExistsColumn(ctx context.Context, driver Driver, tableName, columnName string) (bool, string, string, error) {
	return ExistsColumn(m.GetTables, m.GetColumns, ctx, driver, tableName, columnName)
}

func (m *VastbaseDatabaseMetaData) ExistsIndex(ctx context.Context, driver Driver, tableName, indexName string) (bool, string, string, error) {
	return ExistsIndex(m.GetTables, m.GetIndexes, ctx, driver, tableName, indexName)
}

func (m *VastbaseDatabaseMetaData) ExistsPrimaryKey(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsPrimaryKey(m.GetTables, m.GetPrimaryKeys, ctx, driver, tableName)
}

func (m *VastbaseDatabaseMetaData) Quoter() *Quoter {
	return m.quoter
}

// VastbaseMigratory VastBase迁移实现
type VastbaseMigratory struct {
	DefaultMigratory
}

// NewVastbaseMigratory 创建一个VastBase迁移实现实例
func NewVastbaseMigratory() Migratory {
	return &VastbaseMigratory{
		DefaultMigratory: NewDefaultMigratory("vastbase", NewVastbaseDatabaseMetaData()),
	}
}
