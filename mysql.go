package dbfly

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var MysqlDataTypeMappers = map[string]string{
	Varchar:   "VARCHAR",
	Char:      "CHAR",
	Text:      "MEDIUMTEXT",
	Clob:      "LONGTEXT",
	Boolean:   "TINYINT",
	Tinyint:   "TINYINT",
	Smallint:  "SMALLINT",
	Int:       "INT",
	Bigint:    "BIGINT",
	Decimal:   "DECIMAL",
	Date:      "DATE",
	Time:      "TIME",
	Timestamp: "DATETIME",
	Blob:      "BLOB",
}

// MysqlMigratory Mysql合并实现
type MysqlMigratory struct {
	DefaultMigratory
}

// NewMysqlMigratory 创建一个Mysql合并实现实例
func NewMysqlMigratory() Migratory {
	showTablesSql := "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
	return &MysqlMigratory{
		DefaultMigratory{name: "mysql", showTablesSql: showTablesSql, dataTypeMapper: MysqlDataTypeMappers},
	}
}

func (m *MysqlMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(node.TableName)
	builder.WriteString("\n(\n")
	size := len(node.Columns)
	var pkColumn *ColumnNode
	for index, column := range node.Columns {
		builder.WriteString("  ")
		if pk := m.createTableColumn(column, &builder); pk {
			if pkColumn != nil {
				return errors.New("multiple primary key columns are not allowed to be defined")
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
		builder.WriteString(pkColumn.KeyName)
		builder.WriteString(" PRIMARY KEY (")
		builder.WriteString(pkColumn.ColumnName)
		builder.WriteString(")")
	}
	builder.WriteString("\n)")
	if len(node.Attributes) > 0 {
		for _, attr := range node.Attributes {
			if attr.Dialect != m.Name() {
				continue
			}
			builder.WriteString(" ")
			builder.WriteString(attr.Name)
			builder.WriteString(" = ")
			builder.WriteString(attr.Value)
		}
	}
	if node.Remarks != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(strings.ReplaceAll(node.Remarks, "'", "''"))
		builder.WriteString("'")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) createTableColumn(node *ColumnNode, builder *strings.Builder) bool {
	pk := m.DefaultMigratory.CreateTableColumn(node, builder)

	if node.Remarks != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(strings.ReplaceAll(node.Remarks, "'", "''"))
		builder.WriteString("'")
	}
	return pk
}

func (m *MysqlMigratory) DropIndex(ctx context.Context, driver Driver, node *DropIndexNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP INDEX %s ON %s", node.IndexName, node.TableName))
}

func (m *MysqlMigratory) AddColumn(ctx context.Context, driver Driver, node *AddColumnNode) error {
	for _, column := range node.Columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		builder.WriteString(node.TableName)
		builder.WriteString(" ADD ")
		if pk := m.createTableColumn(column, &builder); pk {
			return errors.New("adding columns is not allowed as a primary key")
		}
		if err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
	}
	return nil
}

func (m *MysqlMigratory) AlterColumn(ctx context.Context, driver Driver, node *AlterColumnNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	builder.WriteString(node.TableName)
	builder.WriteString(" MODIFY ")
	node.Column.ColumnName = node.ColumnName
	if pk := m.createTableColumn(node.Column, &builder); pk {
		return errors.New("alter columns is not allowed as a primary key")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) RenameTable(ctx context.Context, driver Driver, node *RenameTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", node.TableName, node.NewTableName))
}

func (m *MysqlMigratory) AlterTableRemarks(ctx context.Context, driver Driver, node *AlterTableRemarksNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", node.TableName, strings.ReplaceAll(node.Remarks, "'", "''")))
}
