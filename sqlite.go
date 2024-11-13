package dbfly

import (
	"context"
	"fmt"
	"strings"
)

var SqliteDataTypeMappers = map[string]string{
	Varchar:   "VARCHAR",
	Char:      "CHARACTER",
	Text:      "TEXT",
	Clob:      "CLOB",
	Boolean:   "TINYINT",
	Tinyint:   "TINYINT",
	Smallint:  "SMALLINT",
	Int:       "INTEGER",
	Bigint:    "INTEGER",
	Decimal:   "DECIMAL",
	Date:      "DATE",
	Time:      "TIME",
	Timestamp: "DATETIME",
	Blob:      "BLOB",
}

// Sqlite合并实现
type SqliteMigratory struct {
	DefaultMigratory
}

// 创建一个Sqlite合并实现实例
func NewSqliteMigratory() Migratory {
	return &SqliteMigratory{
		DefaultMigratory{name: "sqlite"},
	}
}

// 初始化变更记录表
func (m *SqliteMigratory) InitChangeLogTable(ctx context.Context, driver Driver, changeTableName string) error {
	rows, err := driver.Query(ctx, "SELECT name FROM sqlite_master WHERE type = 'table'")
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
	return driver.Execute(ctx, "CREATE TABLE "+changeTableName+"(id INTEGER PRIMARY KEY AUTOINCREMENT, change_version VARCHAR(255) NOT NULL, is_success TINYINT DEFAULT 0 NOT NULL, created_at DATETIME, updated_at DATETIME)")
}

func (m *SqliteMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
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
	builder.WriteString("\n)")
	return driver.Execute(ctx, builder.String())
}

func (m *SqliteMigratory) createTableColumn(node *ColumnNode, builder *strings.Builder) {
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
		builder.WriteString(columnType(node.DataType, SqliteDataTypeMappers[node.DataType], node.MaxLength, node.NumericScale))
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
}

func (m *SqliteMigratory) CreateIndex(ctx context.Context, driver Driver, node *CreateIndexNode) error {
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

func (m *SqliteMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, node *CreatePrimaryKeyNode) error {
	// 查询表结构
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	pkColumnName := strings.ToLower(node.Column.ColumnName)
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		builder.WriteString(column.Name)
		builder.WriteString(" ")
		builder.WriteString(column.Type)
		if column.DfltValue != "" {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(column.DfltValue)
		}
		if column.Notnull {
			builder.WriteString(" NOT NULL")
		}
		if pkColumnName == strings.ToLower(column.Name) {
			builder.WriteString("CONSTRAINT ")
			builder.WriteString(node.KeyName)
			builder.WriteString(" PRIMARY KEY")
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs)
}

func (m *SqliteMigratory) copyTable(ctx context.Context, driver Driver, createSql string, columnNames []string, tmpTableName, tableName string, indexSqls []string) error {
	if err := driver.Execute(ctx, createSql); err != nil {
		return nil
	}
	columnNameStr := strings.Join(columnNames, ", ")
	if err := driver.Execute(ctx, fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s", tmpTableName, columnNameStr, columnNameStr, tableName)); err != nil {
		return nil
	}
	if err := driver.Execute(ctx, fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		return nil
	}
	if err := driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tmpTableName, tableName)); err != nil {
		return nil
	}
	for _, insexSql := range indexSqls {
		if err := driver.Execute(ctx, insexSql); err != nil {
			return nil
		}
	}
	return nil
}

type sqliteTableStruct struct {
	columns []*sqliteColumnStruct
	indexs  []string
}
type sqliteColumnStruct struct {
	Cid       int
	Name      string
	Type      string
	Notnull   bool
	DfltValue string
	Pk        bool
}

func (m *SqliteMigratory) tableStruct(ctx context.Context, driver Driver, tableName string) (*sqliteTableStruct, error) {
	columns, err := m.parseColumns(ctx, driver, tableName)
	if err != nil {
		return nil, err
	}
	indexSqls, err := m.parseIndexSqls(ctx, driver, tableName)
	if err != nil {
		return nil, err
	}
	return &sqliteTableStruct{
		columns: columns,
		indexs:  indexSqls,
	}, nil
}

func (m *SqliteMigratory) parseColumns(ctx context.Context, driver Driver, tableName string) ([]*sqliteColumnStruct, error) {
	// 查询表结构
	rows, err := driver.Query(ctx, "PRAGMA table_info (?)", tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []*sqliteColumnStruct
	for rows.Next() {
		var column sqliteColumnStruct
		if err := rows.Scan(&column.Cid, &column.Name, &column.Type, &column.Notnull, &column.DfltValue, &column.Pk); err != nil {
			return nil, err
		}
		columns = append(columns, &column)
	}
	return columns, nil
}

func (m *SqliteMigratory) parseIndexSqls(ctx context.Context, driver Driver, tableName string) ([]string, error) {
	rows, err := driver.Query(ctx, "select sql from sqlite_master where sql is not null and type = 'index' and lower(tbl_name) = ?", strings.ToLower(tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sqls []string
	for rows.Next() {
		var sqlStr string
		if err := rows.Scan(&sqlStr); err != nil {
			return nil, err
		}
		sqls = append(sqls, sqlStr)
	}
	return sqls, nil
}

func (m *SqliteMigratory) DropTable(ctx context.Context, driver Driver, node *DropTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP TABLE %s", node.TableName))
}

func (m *SqliteMigratory) DropIndex(ctx context.Context, driver Driver, node *DropIndexNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP INDEX %s", node.IndexName))
}

func (m *SqliteMigratory) AddColumn(ctx context.Context, driver Driver, node *AddColumnNode) error {
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

func (m *SqliteMigratory) AlterColumn(ctx context.Context, driver Driver, node *AlterColumnNode) error {
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	columnName := strings.ToLower(node.ColumnName)
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		if columnName == strings.ToLower(column.Name) {
			m.createTableColumn(node.Column, &builder)
		} else {
			builder.WriteString(column.Name)
			builder.WriteString(" ")
			builder.WriteString(column.Type)
			if column.DfltValue != "" {
				builder.WriteString(" DEFAULT ")
				builder.WriteString(column.DfltValue)
			}
			if column.Notnull {
				builder.WriteString(" NOT NULL")
			}
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs)
}

func (m *SqliteMigratory) DropColumn(ctx context.Context, driver Driver, node *DropColumnNode) error {
	// 查询表结构
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	columnName := strings.ToLower(node.ColumnName)
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		if columnName == strings.ToLower(column.Name) {
			continue
		}
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		builder.WriteString(column.Name)
		builder.WriteString(" ")
		builder.WriteString(column.Type)
		if column.DfltValue != "" {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(column.DfltValue)
		}
		if column.Notnull {
			builder.WriteString(" NOT NULL")
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs)
}

func (m *SqliteMigratory) DropPrimaryKey(ctx context.Context, driver Driver, node *DropPrimaryKeyNode) error {
	// 查询表结构
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		builder.WriteString(column.Name)
		builder.WriteString(" ")
		builder.WriteString(column.Type)
		if column.DfltValue != "" {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(column.DfltValue)
		}
		if column.Notnull {
			builder.WriteString(" NOT NULL")
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs)
}

func (m *SqliteMigratory) RenameTable(ctx context.Context, driver Driver, node *RenameTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("alter table %s rename to %s", node.TableName, node.NewTableName))
}

func (m *SqliteMigratory) AlterTableRemarks(context.Context, Driver, *AlterTableRemarksNode) error {
	// 不支持
	return nil
}
