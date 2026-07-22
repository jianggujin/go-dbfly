package dbfly

import (
	"context"
	sql2 "database/sql"
	"errors"
	"fmt"
	"strings"
)

type MysqlDatabaseMetaData struct {
	quoter *Quoter
	schema string
}

func NewMysqlDatabaseMetaData() *MysqlDatabaseMetaData {
	return &MysqlDatabaseMetaData{
		quoter: NewQuoter('`', '`', AlwaysReserve),
	}
}

func (m *MysqlDatabaseMetaData) Dbms() string {
	return "MySQL"
}

func (m *MysqlDatabaseMetaData) getSchema(ctx context.Context, driver Driver) (string, error) {
	if m.schema == "" {
		// 获取当前使用的SCHEMA
		sql := "SELECT DATABASE()"
		schema, err := doGetScalar[string](ctx, driver, sql)
		if err != nil {
			if errors.Is(err, NoData) {
				return "", Wrap(err, "get current database schema failed")
			}
			return "", err
		}
		m.schema = schema
	}
	return m.schema, nil
}

func (m *MysqlDatabaseMetaData) DataType(str string) string {
	switch str {
	case Varchar:
		return "VARCHAR"
	case Char:
		return "CHAR"
	case Text:
		return "MEDIUMTEXT"
	case Clob:
		return "LONGTEXT"
	case Boolean:
		return "TINYINT"
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

func (m *MysqlDatabaseMetaData) GetTables(ctx context.Context, driver Driver) ([]*Table, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SELECT TABLE_NAME,
       CASE TABLE_TYPE
           WHEN 'BASE TABLE' THEN 'TABLE'
           ELSE TABLE_TYPE END
                     AS TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_TYPE in ('BASE TABLE', 'VIEW')
ORDER BY TABLE_NAME`
	return doGetSlices[Table](ctx, driver, func(rows Rows, t *Table) error {
		return rows.Scan(&t.Name, &t.TableType)
	}, sql, schema)
}

func (m *MysqlDatabaseMetaData) GetColumns(ctx context.Context, driver Driver, tableName string) ([]string, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	// SHOW FULL COLUMNS FROM
	sql := `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	return doGetScalars[string](ctx, driver, sql, schema, tableName)
}

func (m *MysqlDatabaseMetaData) GetIndexes(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SHOW INDEX FROM ` + m.quoter.MustQuote(tableName) + " FROM " + m.quoter.MustQuote(schema)
	var plan *scanPlan
	var (
		keyName    sql2.NullString
		columnName sql2.NullString
	)
	binders := columnBinders{
		"KEY_NAME":    &keyName,
		"COLUMN_NAME": &columnName,
	}
	return doGetSlices[Index](ctx, driver, func(rows Rows, t *Index) error {
		var err error
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		if err = plan.Scan(rows); err != nil {
			return err
		}
		t.Name = keyName.String
		t.ColumnName = columnName.String
		return nil
	}, sql)
}

func (m *MysqlDatabaseMetaData) GetPrimaryKeys(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
	schema, err := m.getSchema(ctx, driver)
	if err != nil {
		return nil, err
	}
	sql := `SHOW KEYS FROM ` + m.quoter.MustQuote(tableName) + " FROM " + m.quoter.MustQuote(schema)
	var plan *scanPlan
	var (
		keyName    sql2.NullString
		columnName sql2.NullString
	)
	binders := columnBinders{
		"KEY_NAME":    &keyName,
		"COLUMN_NAME": &columnName,
	}
	var list []*PrimaryKey
	err = doEach(ctx, driver, func(rows Rows) error {
		var itemErr error
		if plan == nil {
			if plan, itemErr = newScanPlan(rows, binders); itemErr != nil {
				return itemErr
			}
		}
		if itemErr = plan.Scan(rows); itemErr != nil {
			return itemErr
		}
		if !keyName.Valid || (strings.ToUpper(keyName.String) != "PRIMARY" && strings.ToUpper(keyName.String) != "PRI") {
			return nil
		}

		item := new(PrimaryKey)
		item.ColumnName = columnName.String
		item.Name = keyName.String
		list = append(list, item)
		return nil
	}, sql)
	return list, err
}

func (m *MysqlDatabaseMetaData) ExistsTable(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsTable(m.GetTables, ctx, driver, tableName)
}

func (m *MysqlDatabaseMetaData) ExistsColumn(ctx context.Context, driver Driver, tableName, columnName string) (bool, string, string, error) {
	return ExistsColumn(m.GetTables, m.GetColumns, ctx, driver, tableName, columnName)
}

func (m *MysqlDatabaseMetaData) ExistsIndex(ctx context.Context, driver Driver, tableName, indexName string) (bool, string, string, error) {
	return ExistsIndex(m.GetTables, m.GetIndexes, ctx, driver, tableName, indexName)
}

func (m *MysqlDatabaseMetaData) ExistsPrimaryKey(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsPrimaryKey(m.GetTables, m.GetPrimaryKeys, ctx, driver, tableName)
}

func (m *MysqlDatabaseMetaData) Quoter() *Quoter {
	return m.quoter
}

// MysqlMigratory Mysql迁移实现
type MysqlMigratory struct {
	DefaultMigratory
}

// NewMysqlMigratory 创建一个Mysql迁移实现实例
func NewMysqlMigratory() Migratory {
	return &MysqlMigratory{
		DefaultMigratory: NewDefaultMigratory("mysql", NewMysqlDatabaseMetaData()),
	}
}

func (m *MysqlMigratory) CreateTable(ctx context.Context, driver Driver, tableName string, comment string, columns []*ColumnNode, attributes *AttributesNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString("\n(\n")
	size := len(columns)
	var pkColumn *ColumnNode
	for index, column := range columns {
		builder.WriteString("  ")
		if pk := m.createTableColumn(column, &builder); pk {
			if pkColumn != nil {
				return New("multiple primary key columns are not allowed in table %s", tableName)
			}
			if column.KeyName == "" {
				builder.WriteString(" PRIMARY KEY")
			}
			pkColumn = column
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	if pkColumn != nil && pkColumn.KeyName != "" {
		builder.WriteString(",\n  CONSTRAINT ")
		m.QuoteTo(&builder, pkColumn.KeyName)
		builder.WriteString(" PRIMARY KEY (")
		m.QuoteTo(&builder, pkColumn.ColumnName)
		builder.WriteString(")")
	}
	builder.WriteString("\n)")
	if attributes != nil && len(attributes.Attributes) > 0 {
		for _, attr := range attributes.Attributes {
			if attr.Dbms != m.MetaData().Dbms() {
				continue
			}
			builder.WriteString(" ")
			builder.WriteString(attr.Name)
			builder.WriteString(" = ")
			builder.WriteString(attr.Value)
		}
	}
	if comment != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(ReplaceComment(comment))
		builder.WriteString("'")
	}
	_, err := driver.Execute(ctx, builder.String())
	return err
}

func (m *MysqlMigratory) createTableColumn(node *ColumnNode, builder *strings.Builder) bool {
	pk := m.DefaultMigratory.CreateTableColumn(node, builder)

	if node.Comment != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(ReplaceComment(node.Comment))
		builder.WriteString("'")
	}
	return pk
}

func (m *MysqlMigratory) DropIndex(ctx context.Context, driver Driver, tableName, indexName string, _ *AttributesNode) error {
	_, err := driver.Execute(ctx, fmt.Sprintf("DROP INDEX %s ON %s", m.Quote(indexName), m.Quote(tableName)))
	return err
}

func (m *MysqlMigratory) AddColumn(ctx context.Context, driver Driver, tableName string, columns []*AddColumnColumnNode, _ *AttributesNode) error {
	for _, column := range columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		m.QuoteTo(&builder, tableName)
		builder.WriteString(" ADD ")
		m.createAddTableColumn(column, &builder)
		if _, err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
	}
	return nil
}

func (m *MysqlMigratory) createAddTableColumn(node *AddColumnColumnNode, builder *strings.Builder) {
	m.DefaultMigratory.CreateAddTableColumn(node, builder)

	if node.Comment != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(ReplaceComment(node.Comment))
		builder.WriteString("'")
	}
}

func (m *MysqlMigratory) AlterColumn(ctx context.Context, driver Driver, tableName string, columnName string, column *AlterColumnColumnNode, _ *AttributesNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString(" MODIFY ")
	m.createAlterTableColumn(column, &builder, columnName)
	_, err := driver.Execute(ctx, builder.String())
	return err
}

func (m *MysqlMigratory) createAlterTableColumn(node *AlterColumnColumnNode, builder *strings.Builder, columnName string) {
	m.DefaultMigratory.CreateAlterTableColumn(node, builder, columnName)

	if node.Comment != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(ReplaceComment(node.Comment))
		builder.WriteString("'")
	}
}

func (m *MysqlMigratory) RenameTable(ctx context.Context, driver Driver, tableName string, newTableName string, _ *AttributesNode) error {
	_, err := driver.Execute(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", m.Quote(tableName), m.Quote(newTableName)))
	return err
}

func (m *MysqlMigratory) AlterTableComment(ctx context.Context, driver Driver, tableName string, comment string, _ *AttributesNode) error {
	_, err := driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", m.Quote(tableName), ReplaceComment(comment)))
	return err
}
