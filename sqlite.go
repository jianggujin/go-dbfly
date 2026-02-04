package dbfly

import (
	"context"
	"errors"
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

// SqliteMigratory Sqlite合并实现
type SqliteMigratory struct {
	DefaultMigratory
}

// NewSqliteMigratory 创建一个Sqlite合并实现实例
func NewSqliteMigratory() Migratory {
	showTablesSql := "SELECT name FROM sqlite_master WHERE type = 'table'"
	return &SqliteMigratory{
		DefaultMigratory{name: "sqlite", showTablesSql: showTablesSql, dataTypeMapper: SqliteDataTypeMappers},
	}
}

// InitChangeLogTable 初始化变更记录表
func (m *SqliteMigratory) InitChangeLogTable(ctx context.Context, driver Driver, changeTableName string) error {
	rows, err := driver.Query(ctx, "SELECT name FROM sqlite_master WHERE type = 'table'")
	exists := false
	if exists, err = m.ExistsTable(changeTableName, rows, err); err != nil || exists {
		return err
	}
	return m.CreateChangeTable(ctx, driver, changeTableName)
}

func (m *SqliteMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(node.TableName)
	builder.WriteString("\n(\n")
	size := len(node.Columns)
	var pkColumn *ColumnNode
	for index, column := range node.Columns {
		builder.WriteString("  ")
		if pk := m.CreateTableColumn(column, &builder); pk {
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
	if err := driver.Execute(ctx, builder.String()); err != nil {
		return err
	}
	return nil
}

func (m *SqliteMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, node *CreatePrimaryKeyNode) error {
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
	builder.WriteString(",\n  CONSTRAINT ")
	builder.WriteString(node.KeyName)
	builder.WriteString(" PRIMARY KEY (")
	builder.WriteString(node.Column.ColumnName)
	builder.WriteString(")")
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs, nil)
}

func (m *SqliteMigratory) copyTable(ctx context.Context, driver Driver, createSql string, columnNames []string, tmpTableName, tableName string, indexSqls []string, nameMapper map[string]string) error {
	if err := driver.Execute(ctx, createSql); err != nil {
		return nil
	}
	columnNameStr := strings.Join(columnNames, ", ")
	var newColumnNames []string
	if nameMapper == nil || len(nameMapper) == 0 {
		newColumnNames = columnNames
	} else {
		for _, name := range columnNames {
			if value, ok := nameMapper[name]; !ok {
				newColumnNames = append(newColumnNames, value)
				continue
			}
			newColumnNames = append(newColumnNames, name)
		}
	}
	newColumnNameStr := strings.Join(newColumnNames, ", ")
	if err := driver.Execute(ctx, fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s", tmpTableName, newColumnNameStr, columnNameStr, tableName)); err != nil {
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
		if err = rows.Scan(&column.Cid, &column.Name, &column.Type, &column.Notnull, &column.DfltValue, &column.Pk); err != nil {
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
		if err = rows.Scan(&sqlStr); err != nil {
			return nil, err
		}
		sqls = append(sqls, sqlStr)
	}
	return sqls, nil
}

func (m *SqliteMigratory) AddColumn(ctx context.Context, driver Driver, node *AddColumnNode) error {
	for _, column := range node.Columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		builder.WriteString(node.TableName)
		builder.WriteString(" ADD ")
		if pk := m.CreateTableColumn(column, &builder); pk {
			return errors.New("adding columns is not allowed as a primary key")
		}
		if err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
	}
	return nil
}

func (m *SqliteMigratory) RenameColumn(ctx context.Context, driver Driver, node *RenameColumnNode) error {
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
	oldName := strings.ToLower(node.ColumnName)
	newName := node.NewColumnName
	nameMapper := map[string]string{}
	for index, column := range info.columns {
		name := column.Name
		columnNames = append(columnNames, column.Name)
		if oldName == strings.ToLower(column.Name) {
			nameMapper[column.Name] = newName
			name = newName
		}
		builder.WriteString("  ")
		builder.WriteString(name)
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs, nameMapper)
}

func (m *SqliteMigratory) AlterColumn(ctx context.Context, driver Driver, node *AlterColumnNode) error {
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	node.Column.ColumnName = node.ColumnName
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	columnName := strings.ToLower(node.ColumnName)
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		if columnName == strings.ToLower(column.Name) {
			if pk := m.CreateTableColumn(node.Column, &builder); pk {
				return errors.New("alter columns is not allowed as a primary key")
			}
			continue
		}
		builder.WriteString(column.Name)
		builder.WriteString(" ")
		builder.WriteString(column.Type)
		if column.Pk {
			builder.WriteString(" PRIMARY KEY")
		}
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs, nil)
}

func (m *SqliteMigratory) DropColumn(ctx context.Context, driver Driver, node *DropColumnNode) error {
	info, err := m.tableStruct(ctx, driver, node.TableName)
	if err != nil {
		return err
	}
	tmpTableName := node.TableName + "_dbfly"
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tmpTableName)
	builder.WriteString("\n(\n")
	var columnNames []string
	dropColumnName := strings.ToLower(node.ColumnName)
	first := true
	for _, column := range info.columns {
		if dropColumnName == strings.ToLower(column.Name) {
			continue
		}
		if !first {
			builder.WriteString(",\n  ")
		}
		first = false
		columnNames = append(columnNames, column.Name)
		builder.WriteString(column.Name)
		builder.WriteString(" ")
		builder.WriteString(column.Type)
		if column.Pk {
			builder.WriteString(" PRIMARY KEY")
		}
		if column.DfltValue != "" {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(column.DfltValue)
		}
		if column.Notnull {
			builder.WriteString(" NOT NULL")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs, nil)
}

func (m *SqliteMigratory) DropPrimaryKey(ctx context.Context, driver Driver, node *DropPrimaryKeyNode) error {
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, node.TableName, info.indexs, nil)
}

func (m *SqliteMigratory) AlterTableRemarks(context.Context, Driver, *AlterTableRemarksNode) error {
	// 不支持
	return nil
}
