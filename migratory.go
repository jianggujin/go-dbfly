package dbfly

import (
	"context"
	"fmt"
	"strings"
)

// Migratory SQL版本迁移接口
type Migratory interface {
	// Name 迁移器名称
	Name() string
	// CreateTable 创建表
	CreateTable(ctx context.Context, driver Driver, tableName string, comment string, columns []*ColumnNode, attributes *AttributesNode) error
	// CreateIndex 创建索引
	CreateIndex(ctx context.Context, driver Driver, tableName string, indexName string, unique bool, columns []*IndexColumnNode, attributes *AttributesNode) error
	// CreatePrimaryKey 创建主键
	CreatePrimaryKey(ctx context.Context, driver Driver, tableName string, keyName string, columns []*IndexColumnNode, attributes *AttributesNode) error
	// DropTable 删除表
	DropTable(ctx context.Context, driver Driver, tableName string, attributes *AttributesNode) error
	// DropIndex 删除索引
	DropIndex(ctx context.Context, driver Driver, tableName string, indexName string, attributes *AttributesNode) error
	// AddColumn 添加列
	AddColumn(ctx context.Context, driver Driver, tableName string, columns []*AddColumnColumnNode, attributes *AttributesNode) error
	// RenameColumn 重命名列
	RenameColumn(ctx context.Context, driver Driver, tableName string, columnName string, newColumnName string, attributes *AttributesNode) error
	// AlterColumn 修改列
	AlterColumn(ctx context.Context, driver Driver, tableName string, columnName string, column *AlterColumnColumnNode, attributes *AttributesNode) error
	// DropColumn 删除列
	DropColumn(ctx context.Context, driver Driver, tableName string, columnName string, attributes *AttributesNode) error
	// DropPrimaryKey 删除主键
	DropPrimaryKey(ctx context.Context, driver Driver, tableName string, attributes *AttributesNode) error
	// RenameTable 重命名表
	RenameTable(ctx context.Context, driver Driver, tableName string, newTableName string, attributes *AttributesNode) error
	// AlterTableComment 修改表说明
	AlterTableComment(ctx context.Context, driver Driver, tableName string, comment string, attributes *AttributesNode) error
	// Script 执行自定义SQL脚本
	Script(ctx context.Context, driver Driver, script string) error
	// SplitSQLStatements 拆分SQL脚本为单个执行单元，可由数据库实现覆盖
	SplitSQLStatements(script string) []string
	// MetaData 元数据信息
	MetaData() DatabaseMetaData
}

type DefaultMigratory struct {
	name     string
	metaData DatabaseMetaData
	logger   Logger
}

func NewDefaultMigratory(name string, metaData DatabaseMetaData) DefaultMigratory {
	return DefaultMigratory{
		name:     name,
		metaData: metaData,
		logger:   nopLogger{},
	}
}

// NewDefaultMigratoryWithLogger 创建带有日志器的迁移器
func NewDefaultMigratoryWithLogger(name string, metaData DatabaseMetaData, logger Logger) DefaultMigratory {
	return DefaultMigratory{
		name:     name,
		metaData: metaData,
		logger:   logger,
	}
}

// SetLogger 设置日志器
func (m *DefaultMigratory) SetLogger(logger Logger) {
	m.logger = logger
}

func (m *DefaultMigratory) Name() string {
	return m.name
}

func (m *DefaultMigratory) Quote(str string) string {
	return m.metaData.Quoter().MustQuote(str)
}

func (m *DefaultMigratory) QuoteTo(buf *strings.Builder, value string) {
	m.metaData.Quoter().MustQuoteTo(buf, value)
}

func (m *DefaultMigratory) DataType(str string) string {
	return m.metaData.DataType(str)
}

func (m *DefaultMigratory) CreateTable(ctx context.Context, driver Driver, tableName string, comment string, columns []*ColumnNode, _ *AttributesNode) error {
	m.logger.Debug("create table", "tableName", tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString("\n(\n")
	size := len(columns)
	var pkColumn *ColumnNode
	for index, column := range columns {
		builder.WriteString("  ")
		if pk := m.CreateTableColumn(column, &builder); pk {
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
	if err := driver.Execute(ctx, builder.String()); err != nil {
		return err
	}

	if comment != "" {
		commentSQL := fmt.Sprintf("COMMENT ON TABLE %s IS '%s'",
			m.Quote(tableName), ReplaceComment(comment))
		if err := driver.Execute(ctx, commentSQL); err != nil {
			return err
		}
	}
	for _, column := range columns {
		if comment != "" {
			columnCommentSQL := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
				m.Quote(tableName), m.Quote(column.ColumnName), ReplaceComment(column.Comment))
			if err := driver.Execute(ctx, columnCommentSQL); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *DefaultMigratory) CreateTableColumn(node *ColumnNode, builder *strings.Builder) bool {
	var dbmsNode *ColumnDbmsNode

	// 查找方言
	for _, dbms := range node.ColumnDbms {
		if dbms.Dbms == m.MetaData().Dbms() {
			dbmsNode = dbms
			break
		}
	}
	m.QuoteTo(builder, node.ColumnName)
	builder.WriteString(" ")
	var defaultValue string
	if dbmsNode != nil {
		builder.WriteString(dbmsNode.DataType)
		if dbmsNode.DefaultOriginValue != "" {
			defaultValue = dbmsNode.DefaultOriginValue
		} else if dbmsNode.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(dbmsNode.DefaultValue, "'", "''"))
		}
	} else {
		builder.WriteString(columnType(node.DataType, m.DataType(node.DataType), node.MaxLength, node.NumericScale))
		if node.DefaultOriginValue != "" {
			defaultValue = node.DefaultOriginValue
		} else if node.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(node.DefaultValue, "'", "''"))
		}
	}
	if node.PrimaryKey {
		// builder.WriteString(" PRIMARY KEY")
		return true
	}
	if defaultValue != "" {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(defaultValue)
	}
	if node.Unique {
		builder.WriteString(" UNIQUE")
	}
	if !node.Nullable {
		builder.WriteString(" NOT NULL")
	}
	return false
}

func (m *DefaultMigratory) CreateIndex(ctx context.Context, driver Driver, tableName string, indexName string, unique bool, columns []*IndexColumnNode, _ *AttributesNode) error {
	uniqueStr := "false"
	if unique {
		uniqueStr = "true"
	}
	m.logger.Debug("create index", "tableName", tableName, "indexName", indexName, "unique", uniqueStr)
	var builder strings.Builder
	builder.WriteString("CREATE")
	if unique {
		builder.WriteString(" UNIQUE")
	}
	builder.WriteString(" INDEX ")
	m.QuoteTo(&builder, indexName)
	builder.WriteString(" ON ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString(" (")
	var columnNames []string
	for _, columnNode := range columns {
		columnNames = append(columnNames, columnNode.Name)
	}
	m.metaData.Quoter().MustJoinWrite(&builder, columnNames, ", ")
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, tableName string, keyName string, columns []*IndexColumnNode, _ *AttributesNode) error {
	m.logger.Debug("create primary key", "tableName", tableName, "keyName", keyName)
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString(" ADD CONSTRAINT ")
	m.QuoteTo(&builder, keyName)
	builder.WriteString(" PRIMARY KEY (")
	var columnNames []string
	for _, columnNode := range columns {
		columnNames = append(columnNames, columnNode.Name)
	}
	m.metaData.Quoter().MustJoinWrite(&builder, columnNames, ", ")
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) DropTable(ctx context.Context, driver Driver, tableName string, _ *AttributesNode) error {
	m.logger.Debug("drop table", "tableName", tableName)
	sql := fmt.Sprintf("DROP TABLE %s", m.Quote(tableName))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) DropIndex(ctx context.Context, driver Driver, _, indexName string, _ *AttributesNode) error {
	m.logger.Debug("drop index", "indexName", indexName)
	sql := fmt.Sprintf("DROP INDEX %s", m.Quote(indexName))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) AddColumn(ctx context.Context, driver Driver, tableName string, columns []*AddColumnColumnNode, _ *AttributesNode) error {
	m.logger.Debug("add column", "tableName", tableName, "columnCount", len(columns))
	for _, column := range columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		m.QuoteTo(&builder, tableName)
		builder.WriteString(" ADD ")
		m.CreateAddTableColumn(column, &builder)
		if err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
		if column.Comment != "" {
			commentSQL := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
				m.Quote(tableName), m.Quote(column.ColumnName), ReplaceComment(column.Comment))
			if err := driver.Execute(ctx, commentSQL); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *DefaultMigratory) CreateAddTableColumn(node *AddColumnColumnNode, builder *strings.Builder) {
	var dbmsNode *ColumnDbmsNode

	// 查找方言
	for _, dbms := range node.ColumnDbms {
		if dbms.Dbms == m.MetaData().Dbms() {
			dbmsNode = dbms
			break
		}
	}
	m.QuoteTo(builder, node.ColumnName)
	builder.WriteString(" ")
	var defaultValue string
	if dbmsNode != nil {
		builder.WriteString(dbmsNode.DataType)
		if dbmsNode.DefaultOriginValue != "" {
			defaultValue = dbmsNode.DefaultOriginValue
		} else if dbmsNode.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(dbmsNode.DefaultValue, "'", "''"))
		}
	} else {
		builder.WriteString(columnType(node.DataType, m.DataType(node.DataType), node.MaxLength, node.NumericScale))
		if node.DefaultOriginValue != "" {
			defaultValue = node.DefaultOriginValue
		} else if node.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(node.DefaultValue, "'", "''"))
		}
	}
	if defaultValue != "" {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(defaultValue)
	}
	if node.Unique {
		builder.WriteString(" UNIQUE")
	}
	if !node.Nullable {
		builder.WriteString(" NOT NULL")
	}
}

func (m *DefaultMigratory) RenameColumn(ctx context.Context, driver Driver, tableName string, columnName string, newColumnName string, _ *AttributesNode) error {
	m.logger.Debug("rename column", "tableName", tableName, "columnName", columnName, "newColumnName", newColumnName)
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString(" RENAME COLUMN ")
	m.QuoteTo(&builder, columnName)
	builder.WriteString(" TO ")
	m.QuoteTo(&builder, newColumnName)
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) AlterColumn(ctx context.Context, driver Driver, tableName string, columnName string, column *AlterColumnColumnNode, _ *AttributesNode) error {
	m.logger.Debug("alter column", "tableName", tableName, "columnName", columnName)
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.QuoteTo(&builder, tableName)
	builder.WriteString(" MODIFY ")
	m.CreateAlterTableColumn(column, &builder, columnName)
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) CreateAlterTableColumn(node *AlterColumnColumnNode, builder *strings.Builder, columnName string) {
	var dbmsNode *ColumnDbmsNode

	// 查找方言
	for _, dbms := range node.ColumnDbms {
		if dbms.Dbms == m.MetaData().Dbms() {
			dbmsNode = dbms
			break
		}
	}
	m.QuoteTo(builder, columnName)
	builder.WriteString(" ")
	var defaultValue string
	if dbmsNode != nil {
		builder.WriteString(dbmsNode.DataType)
		if dbmsNode.DefaultOriginValue != "" {
			defaultValue = dbmsNode.DefaultOriginValue
		} else if dbmsNode.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(dbmsNode.DefaultValue, "'", "''"))
		}
	} else {
		builder.WriteString(columnType(node.DataType, m.DataType(node.DataType), node.MaxLength, node.NumericScale))
		if node.DefaultOriginValue != "" {
			defaultValue = node.DefaultOriginValue
		} else if node.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(node.DefaultValue, "'", "''"))
		}
	}
	if defaultValue != "" {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(defaultValue)
	}
	if node.Unique {
		builder.WriteString(" UNIQUE")
	}
	if !node.Nullable {
		builder.WriteString(" NOT NULL")
	}
}

func (m *DefaultMigratory) DropColumn(ctx context.Context, driver Driver, tableName string, columnName string, _ *AttributesNode) error {
	m.logger.Debug("drop column", "tableName", tableName, "columnName", columnName)
	sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", m.Quote(tableName), m.Quote(columnName))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) DropPrimaryKey(ctx context.Context, driver Driver, tableName string, _ *AttributesNode) error {
	m.logger.Debug("drop primary key", "tableName", tableName)
	sql := fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", m.Quote(tableName))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) RenameTable(ctx context.Context, driver Driver, tableName string, newTableName string, _ *AttributesNode) error {
	m.logger.Debug("rename table", "tableName", tableName, "newTableName", newTableName)
	sql := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", m.Quote(tableName), m.Quote(newTableName))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) AlterTableComment(ctx context.Context, driver Driver, tableName string, comment string, _ *AttributesNode) error {
	m.logger.Debug("alter table comment", "tableName", tableName)
	sql := fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", m.Quote(tableName), ReplaceComment(comment))
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) Script(ctx context.Context, driver Driver, script string) error {
	m.logger.Debug("execute script")
	for _, statement := range m.SplitSQLStatements(script) {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := driver.Execute(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (m *DefaultMigratory) SplitSQLStatements(script string) []string {
	return splitSQLStatements(script)
}

func (m *DefaultMigratory) MetaData() DatabaseMetaData {
	return m.metaData
}
