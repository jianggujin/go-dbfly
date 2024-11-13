package dbfly

import (
	"context"
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

// Mysql合并实现
type MysqlMigratory struct {
	DefaultMigratory
}

// 创建一个Mysql合并实现实例
func NewMysqlMigratory() Migratory {
	return &MysqlMigratory{
		DefaultMigratory{name: "mysql"},
	}
}

// 初始化变更记录表
func (m *MysqlMigratory) InitChangeLogTable(ctx context.Context, driver Driver, changeTableName string) error {
	rows, err := driver.Query(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		if changeTableName == strings.ToLower(tableName) {
			return nil
		}
	}
	return driver.Execute(ctx, "CREATE TABLE "+changeTableName+"(id BIGINT PRIMARY KEY AUTO_INCREMENT, change_version VARCHAR(255) NOT NULL, is_success TINYINT DEFAULT 0 NOT NULL, created_at DATETIME, updated_at DATETIME) ENGINE = InnoDB")
}

func (m *MysqlMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("create table ")
	builder.WriteString(node.TableName)
	builder.WriteString("\n(\n")
	size := len(node.Columns)
	for index, column := range node.Columns {
		builder.WriteString("  ")
		m.createTableColumn(column, &builder)
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n) ENGINE = InnoDB\n  DEFAULT CHARSET = utf8mb4")
	if node.Remarks != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(strings.ReplaceAll(node.Remarks, "'", "''"))
		builder.WriteString("'")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) createTableColumn(node *ColumnNode, builder *strings.Builder) {
	var dialectNode *ColumnDialectNode
	// 查找方言
	for _, dialect := range node.Dialects {
		if dialect.Dialect == m.Name() {
			dialectNode = dialect
			break
		}
	}
	builder.WriteString(node.ColumnName)
	builder.WriteString(" ")
	var defaultValue string
	if dialectNode != nil {
		builder.WriteString(dialectNode.DataType)
		if dialectNode.DefaultOriginValue != "" {
			defaultValue = dialectNode.DefaultOriginValue
		} else if dialectNode.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(dialectNode.DefaultValue, "'", "''"))
		}
	} else {
		builder.WriteString(columnType(node.DataType, MysqlDataTypeMappers[node.DataType], node.MaxLength, node.NumericScale))
		if node.DefaultOriginValue != "" {
			defaultValue = node.DefaultOriginValue
		} else if node.DefaultValue != "" {
			defaultValue = fmt.Sprintf("'%s'", strings.ReplaceAll(node.DefaultValue, "'", "''"))
		}
	}
	if node.PrimaryKey {
		builder.WriteString(" PRIMARY KEY")
	} else {
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
	if node.Remarks != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(strings.ReplaceAll(node.Remarks, "'", "''"))
		builder.WriteString("'")
	}
}

func (m *MysqlMigratory) CreateIndex(ctx context.Context, driver Driver, node *CreateIndexNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE")
	if node.Unique {
		builder.WriteString(" UNIQUE")
	}
	builder.WriteString(" INDEX ")
	builder.WriteString(node.IndexName)
	builder.WriteString(" ON ")
	builder.WriteString(node.TableName)
	builder.WriteString(" (")
	var columns []string
	for _, columnNode := range node.Columns {
		columns = append(columns, columnNode.ColumnName)
	}
	builder.WriteString(strings.Join(columns, ", "))
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, node *CreatePrimaryKeyNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	builder.WriteString(node.TableName)
	builder.WriteString(" ADD CONSTRAINT ")
	builder.WriteString(node.KeyName)
	builder.WriteString(" PRIMARY KEY (")
	builder.WriteString(node.Column.ColumnName)
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) DropTable(ctx context.Context, driver Driver, node *DropTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP TABLE %s", node.TableName))
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
		m.createTableColumn(column, &builder)
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
	if node.ColumnName == node.Column.ColumnName {
		builder.WriteString(" MODIFY ")
	} else {
		builder.WriteString(" CHANGE ")
		builder.WriteString(node.ColumnName)
		builder.WriteString(" ")
	}
	m.createTableColumn(node.Column, &builder)
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) DropColumn(ctx context.Context, driver Driver, node *DropColumnNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", node.TableName, node.ColumnName))
}

func (m *MysqlMigratory) DropPrimaryKey(ctx context.Context, driver Driver, node *DropPrimaryKeyNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", node.TableName))
}

func (m *MysqlMigratory) RenameTable(ctx context.Context, driver Driver, node *RenameTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", node.TableName, node.NewTableName))
}

func (m *MysqlMigratory) AlterTableRemarks(ctx context.Context, driver Driver, node *AlterTableRemarksNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", node.TableName, strings.ReplaceAll(node.Remarks, "'", "''")))
}
