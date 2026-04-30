package dbfly

import (
	"context"
	"crypto/rand"
	sql2 "database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

func sqliteTmpTableName(tableName string) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate random suffix for temp table: %v", err))
	}
	return fmt.Sprintf("%s_dbfly_%s", tableName, hex.EncodeToString(b))
}

type SqliteDatabaseMetaData struct {
	quoter *Quoter
}

func NewSqliteDatabaseMetaData() *SqliteDatabaseMetaData {
	return &SqliteDatabaseMetaData{
		quoter: NewQuoter('`', '`', AlwaysReserve),
	}
}

func (m *SqliteDatabaseMetaData) Dbms() string {
	return "SQLite"
}

func (m *SqliteDatabaseMetaData) DataType(str string) string {
	switch str {
	case Varchar:
		return "VARCHAR"
	case Char:
		return "CHARACTER"
	case Text:
		return "TEXT"
	case Clob:
		return "CLOB"
	case Boolean:
		return "TINYINT"
	case Tinyint:
		return "TINYINT"
	case Smallint:
		return "SMALLINT"
	case Int:
		return "INTEGER"
	case Bigint:
		return "INTEGER"
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

func (m *SqliteDatabaseMetaData) GetTables(ctx context.Context, driver Driver) ([]*Table, error) {
	sql := `PRAGMA table_list`
	var plan *scanPlan
	var (
		name sql2.NullString
		Type sql2.NullString
	)
	binders := columnBinders{
		"NAME": &name,
		"TYPE": &Type,
	}
	var list []*Table
	err := doEach(ctx, driver, func(rows Rows) error {
		var err error
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		lowerName := sqliteUnquoteIdentifier(strings.ToLower(name.String))
		if "sqlite_sequence" == lowerName || "sqlite_schema" == lowerName || "sqlite_temp_schema" == lowerName {
			return nil
		}
		upperType := strings.ToUpper(Type.String)
		if "TABLE" != upperType && "VIEW" != upperType {
			return nil
		}
		list = append(list, &Table{
			Name:      sqliteUnquoteIdentifier(name.String),
			TableType: upperType,
		})
		return nil
	}, sql)
	return list, err
}

func (m *SqliteDatabaseMetaData) GetColumns(ctx context.Context, driver Driver, tableName string) ([]string, error) {
	sql := "PRAGMA table_info (?)"
	var plan *scanPlan
	var (
		name sql2.NullString
	)
	binders := columnBinders{
		"NAME": &name,
	}
	var list []string
	err := doEach(ctx, driver, func(rows Rows) error {
		var err error
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		list = append(list, sqliteUnquoteIdentifier(name.String))
		return nil
	}, sql, tableName)
	return list, err
}

func (m *SqliteDatabaseMetaData) GetIndexes(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
	sql := "PRAGMA index_list (?)"
	var plan *scanPlan
	var (
		name sql2.NullString
	)
	binders := columnBinders{
		"NAME": &name,
	}
	var list []string
	if err := doEach(ctx, driver, func(rows Rows) error {
		var err error
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		list = append(list, name.String)
		return nil
	}, sql, tableName); err != nil {
		return nil, err
	}
	var indexes []*Index
	for _, index := range list {
		columns, err := m.getIndexInfo(ctx, driver, index)
		if err != nil {
			return nil, err
		}
		for _, column := range columns {
			indexes = append(indexes, &Index{
				Name:       index,
				ColumnName: column,
			})
		}
	}
	return indexes, nil
}

func (m *SqliteDatabaseMetaData) getIndexInfo(ctx context.Context, driver Driver, indexName string) ([]string, error) {
	sql := "PRAGMA index_info (?)"
	var plan *scanPlan
	var (
		name sql2.NullString
	)
	binders := columnBinders{
		"NAME": &name,
	}
	var list []string
	err := doEach(ctx, driver, func(rows Rows) error {
		var err error
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		list = append(list, sqliteUnquoteIdentifier(name.String))
		return nil
	}, sql, indexName)
	return list, err
}

// sqliteUnquoteIdentifier 去除标识符的引号
func sqliteUnquoteIdentifier(name string) string {
	if name == "" {
		return name
	}

	name = strings.TrimSpace(name)

	if len(name) > 2 {
		first := name[0]
		last := name[len(name)-1]

		// 检查是否被 ` `, " ", 或 [ ] 包围
		if (first == '`' && last == '`') ||
			(first == '"' && last == '"') ||
			(first == '[' && last == ']') {
			// 去引号，与 getColumns() 返回的列名保持一致
			name = name[1 : len(name)-1]
		}
	}

	return name
}

func (m *SqliteDatabaseMetaData) GetPrimaryKeys(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
	sql := `select sql from sqlite_schema where lower(name) = lower(?) and type in ('table', 'view')`
	sqlStr, err := doGetScalar[string](ctx, driver, sql, tableName)
	if err != nil {
		return nil, err
	}
	// PKUnnamedPattern 用于提取未命名主键
	PKUnnamedPattern := regexp.MustCompile(`(?is).*PRIMARY\s+KEY\s*\((.*?)\).*`)
	// PKNamedPattern 用于提取命名主键
	PKNamedPattern := regexp.MustCompile(`(?is).*CONSTRAINT\s*(.*?)\s*PRIMARY\s+KEY\s*\((.*?)\).*`)
	// 辅助函数：分割列名
	splitColumns := func(columnsStr string) []string {
		parts := strings.Split(columnsStr, ",")
		for i, part := range parts {
			parts[i] = sqliteUnquoteIdentifier(strings.TrimSpace(part))
		}
		return parts
	}

	// 示例函数：解析主键信息
	parsePrimaryKey := func(sql string) (pkName string, pkColumns []string) {
		// 首先尝试匹配命名主键
		if matches := PKNamedPattern.FindStringSubmatch(sql); matches != nil {
			pkName = sqliteUnquoteIdentifier(matches[1])
			pkColumns = splitColumns(matches[2])
			return
		}

		// 尝试匹配未命名主键
		if matches := PKUnnamedPattern.FindStringSubmatch(sql); matches != nil {
			pkColumns = splitColumns(matches[1])
		}
		return
	}
	var primaryKeys []*PrimaryKey
	pkName, pkColumns := parsePrimaryKey(sqlStr)
	if len(pkColumns) > 0 {
		for _, column := range pkColumns {
			primaryKeys = append(primaryKeys, &PrimaryKey{
				Name:       pkName,
				ColumnName: column,
			})
		}
		return primaryKeys, nil
	}

	sql = "PRAGMA table_info (?)"
	var plan *scanPlan
	var (
		name sql2.NullString
		pk   sql2.NullInt32
	)
	binders := columnBinders{
		"NAME": &name,
		"PK":   &pk,
	}
	err = doEach(ctx, driver, func(rows Rows) error {
		if plan == nil {
			if plan, err = newScanPlan(rows, binders); err != nil {
				return err
			}
		}
		if pk.Int32 == 1 {
			primaryKeys = append(primaryKeys, &PrimaryKey{
				Name:       pkName,
				ColumnName: sqliteUnquoteIdentifier(name.String),
			})
		}
		return nil
	}, sql, tableName)
	return primaryKeys, err
}

func (m *SqliteDatabaseMetaData) ExistsTable(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsTable(m.GetTables, ctx, driver, tableName)
}

func (m *SqliteDatabaseMetaData) ExistsColumn(ctx context.Context, driver Driver, tableName, columnName string) (bool, string, string, error) {
	return ExistsColumn(m.GetTables, m.GetColumns, ctx, driver, tableName, columnName)
}

func (m *SqliteDatabaseMetaData) ExistsIndex(ctx context.Context, driver Driver, tableName, indexName string) (bool, string, string, error) {
	return ExistsIndex(m.GetTables, m.GetIndexes, ctx, driver, tableName, indexName)
}

func (m *SqliteDatabaseMetaData) ExistsPrimaryKey(ctx context.Context, driver Driver, tableName string) (bool, string, error) {
	return ExistsPrimaryKey(m.GetTables, m.GetPrimaryKeys, ctx, driver, tableName)
}

func (m *SqliteDatabaseMetaData) Quoter() *Quoter {
	return m.quoter
}

// SqliteMigratory Sqlite迁移实现
type SqliteMigratory struct {
	DefaultMigratory
}

// NewSqliteMigratory 创建一个Sqlite迁移实现实例
func NewSqliteMigratory() Migratory {
	return &SqliteMigratory{
		DefaultMigratory: NewDefaultMigratory("sqlite", NewSqliteDatabaseMetaData()),
	}
}

func (m *SqliteMigratory) CreateTable(ctx context.Context, driver Driver, tableName string, comment string, columns []*ColumnNode, _ *AttributesNode) error {
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
	return nil
}

func (m *SqliteMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, tableName string, keyName string, columns []*IndexColumnNode, _ *AttributesNode) error {
	info, err := m.tableStruct(ctx, driver, tableName)
	if err != nil {
		return err
	}
	tmpTableName := sqliteTmpTableName(tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		m.QuoteTo(&builder, column.Name)
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
	m.QuoteTo(&builder, keyName)
	builder.WriteString(" PRIMARY KEY (")
	var pkNames []string
	for _, columnNode := range columns {
		pkNames = append(pkNames, columnNode.Name)
	}
	m.metaData.Quoter().MustJoinWrite(&builder, pkNames, ", ")
	builder.WriteString(")")
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, tableName, info.indexs, nil)
}

func (m *SqliteMigratory) copyTable(ctx context.Context, driver Driver, createSql string, columnNames []string, tmpTableName, tableName string, indexSqls []string, nameMapper map[string]string) error {
	if err := driver.Execute(ctx, createSql); err != nil {
		return err
	}
	columnNameStr := m.metaData.Quoter().MustJoin(columnNames, ", ")
	var newColumnNames []string
	if nameMapper == nil || len(nameMapper) == 0 {
		newColumnNames = columnNames
	} else {
		for _, name := range columnNames {
			if value, ok := nameMapper[name]; ok {
				newColumnNames = append(newColumnNames, value)
			} else {
				newColumnNames = append(newColumnNames, name)
			}
		}
	}
	newColumnNameStr := m.metaData.Quoter().MustJoin(newColumnNames, ", ")
	if err := driver.Execute(ctx, fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM %s", m.Quote(tmpTableName), newColumnNameStr, columnNameStr, m.Quote(tableName))); err != nil {
		return err
	}
	if err := driver.Execute(ctx, fmt.Sprintf("DROP TABLE %s", m.Quote(tableName))); err != nil {
		return err
	}
	if err := driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", m.Quote(tmpTableName), m.Quote(tableName))); err != nil {
		return err
	}
	for _, indexSql := range indexSqls {
		if err := driver.Execute(ctx, indexSql); err != nil {
			return err
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
		column := new(sqliteColumnStruct)
		if err = rows.Scan(&column.Cid, &column.Name, &column.Type, &column.Notnull, &column.DfltValue, &column.Pk); err != nil {
			return nil, err
		}
		column.Name = sqliteUnquoteIdentifier(column.Name)
		columns = append(columns, column)
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

func (m *SqliteMigratory) AddColumn(ctx context.Context, driver Driver, tableName string, columns []*AddColumnColumnNode, _ *AttributesNode) error {
	for _, column := range columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		m.QuoteTo(&builder, tableName)
		builder.WriteString(" ADD ")
		m.CreateAddTableColumn(column, &builder)
		if err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
	}
	return nil
}

func (m *SqliteMigratory) RenameColumn(ctx context.Context, driver Driver, tableName string, columnName string, newColumnName string, _ *AttributesNode) error {
	info, err := m.tableStruct(ctx, driver, tableName)
	if err != nil {
		return err
	}
	tmpTableName := sqliteTmpTableName(tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	oldName := strings.ToLower(columnName)
	newName := newColumnName
	nameMapper := map[string]string{}
	for index, column := range info.columns {
		name := column.Name
		columnNames = append(columnNames, column.Name)
		if oldName == strings.ToLower(column.Name) {
			nameMapper[column.Name] = newName
			name = newName
		}
		builder.WriteString("  ")
		m.QuoteTo(&builder, name)
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, tableName, info.indexs, nameMapper)
}

func (m *SqliteMigratory) AlterColumn(ctx context.Context, driver Driver, tableName string, columnName string, column *AlterColumnColumnNode, _ *AttributesNode) error {
	info, err := m.tableStruct(ctx, driver, tableName)
	if err != nil {
		return err
	}
	tmpTableName := sqliteTmpTableName(tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	lowerColumnName := strings.ToLower(columnName)
	for index, columnInfo := range info.columns {
		columnNames = append(columnNames, columnInfo.Name)
		builder.WriteString("  ")
		if lowerColumnName == strings.ToLower(columnInfo.Name) {
			m.CreateAlterTableColumn(column, &builder, columnName)
			continue
		}
		m.QuoteTo(&builder, columnInfo.Name)
		builder.WriteString(" ")
		builder.WriteString(columnInfo.Type)
		if columnInfo.Pk {
			builder.WriteString(" PRIMARY KEY")
		}
		if columnInfo.DfltValue != "" {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(columnInfo.DfltValue)
		}
		if columnInfo.Notnull {
			builder.WriteString(" NOT NULL")
		}
		if index < size-1 {
			builder.WriteString(",\n")
		}
	}
	builder.WriteString("\n)")
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, tableName, info.indexs, nil)
}

func (m *SqliteMigratory) DropColumn(ctx context.Context, driver Driver, tableName string, columnName string, _ *AttributesNode) error {
	info, err := m.tableStruct(ctx, driver, tableName)
	if err != nil {
		return err
	}
	tmpTableName := sqliteTmpTableName(tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tmpTableName)
	builder.WriteString("\n(\n")
	var columnNames []string
	dropColumnName := strings.ToLower(columnName)
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
		m.QuoteTo(&builder, column.Name)
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, tableName, info.indexs, nil)
}

func (m *SqliteMigratory) DropPrimaryKey(ctx context.Context, driver Driver, tableName string, _ *AttributesNode) error {
	info, err := m.tableStruct(ctx, driver, tableName)
	if err != nil {
		return err
	}
	tmpTableName := sqliteTmpTableName(tableName)
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.QuoteTo(&builder, tmpTableName)
	builder.WriteString("\n(\n")
	size := len(info.columns)
	var columnNames []string
	for index, column := range info.columns {
		columnNames = append(columnNames, column.Name)
		builder.WriteString("  ")
		m.QuoteTo(&builder, column.Name)
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
	return m.copyTable(ctx, driver, builder.String(), columnNames, tmpTableName, tableName, info.indexs, nil)
}

func (m *SqliteMigratory) AlterTableComment(_ context.Context, _ Driver, _ string, _ string, _ *AttributesNode) error {
	return nil
}
