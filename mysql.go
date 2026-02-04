package dbfly

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// MysqlMigratory Mysql合并实现
type MysqlMigratory struct {
	DefaultMigratory
}

// NewMysqlMigratory 创建一个Mysql合并实现实例
func NewMysqlMigratory() Migratory {
	showTablesSql := "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
	dataTypeMappers := map[string]string{
		Varchar: "VARCHAR", Char: "CHAR", Text: "MEDIUMTEXT", Clob: "LONGTEXT", Boolean: "TINYINT", Tinyint: "TINYINT", Smallint: "SMALLINT",
		Int: "INT", Bigint: "BIGINT", Decimal: "DECIMAL", Date: "DATE", Time: "TIME", Timestamp: "DATETIME", Blob: "BLOB",
	}
	reservedWords := []string{
		"ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB",
		"BOTH", "BY", "CALL", "CASCADE", "CASE", "CHAIN", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
		"CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE",
		"DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC",
		"DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS",
		"EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT",
		"GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE",
		"IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8",
		"INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LABEL", "LEADING", "LEAVE", "LEFT",
		"LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT",
		"LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND",
		"MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON	OPTIMIZE",
		"OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
		"RAID0", "RANGE", "RANK", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE",
		"REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT",
		"SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
		"SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN",
		"TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO",
		"UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
		"VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "X509",
		"XOR", "YEAR_MONTH", "ZEROFILL"}
	return &MysqlMigratory{
		DefaultMigratory: NewDefaultMigratory("mysql", showTablesSql, dataTypeMappers, NewQuoter('`', '`', AlwaysReserve), reservedWords...),
	}
}

func (m *MysqlMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.quoteTo(&builder, node.TableName)
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
		m.quoteTo(&builder, pkColumn.KeyName)
		builder.WriteString(" PRIMARY KEY (")
		m.quoteTo(&builder, pkColumn.ColumnName)
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
		builder.WriteString(ReplaceRemarks(node.Remarks))
		builder.WriteString("'")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) createTableColumn(node *ColumnNode, builder *strings.Builder) bool {
	pk := m.DefaultMigratory.CreateTableColumn(node, builder)

	if node.Remarks != "" {
		builder.WriteString(" COMMENT '")
		builder.WriteString(ReplaceRemarks(node.Remarks))
		builder.WriteString("'")
	}
	return pk
}

func (m *MysqlMigratory) DropIndex(ctx context.Context, driver Driver, node *DropIndexNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP INDEX %s ON %s", m.quote(node.IndexName), m.quote(node.TableName)))
}

func (m *MysqlMigratory) AddColumn(ctx context.Context, driver Driver, node *AddColumnNode) error {
	for _, column := range node.Columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		m.quoteTo(&builder, node.TableName)
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
	m.quoteTo(&builder, node.TableName)
	builder.WriteString(" MODIFY ")
	node.Column.ColumnName = node.ColumnName
	if pk := m.createTableColumn(node.Column, &builder); pk {
		return errors.New("alter columns is not allowed as a primary key")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *MysqlMigratory) RenameTable(ctx context.Context, driver Driver, node *RenameTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", m.quote(node.TableName), m.quote(node.NewTableName)))
}

func (m *MysqlMigratory) AlterTableRemarks(ctx context.Context, driver Driver, node *AlterTableRemarksNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", m.quote(node.TableName), ReplaceRemarks(node.Remarks)))
}
