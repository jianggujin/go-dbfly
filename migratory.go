package dbfly

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-version"
	"sort"
	"strings"
	"time"
)

// QuotePolicy describes quote handle policy
type QuotePolicy int

// All QuotePolicies
const (
	QuotePolicyAlways QuotePolicy = iota
	QuotePolicyNone
	QuotePolicyReserved
)

const allDialect = "$all"
const (
	Varchar   = "VARCHAR"
	Char      = "CHAR"
	Text      = "TEXT"
	Clob      = "CLOB"
	Boolean   = "BOOLEAN"
	Tinyint   = "TINYINT"
	Smallint  = "SMALLINT"
	Int       = "INT"
	Bigint    = "BIGINT"
	Decimal   = "DECIMAL"
	Date      = "DATE"
	Time      = "TIME"
	Timestamp = "TIMESTAMP"
	Blob      = "BLOB"
)

const (
	COLUMN_ID             = "ID"
	COLUMN_CHANGE_VERSION = "CHANGE_VERSION"
	COLUMN_IS_SUCCESS     = "IS_SUCCESS"
	COLUMN_CREATED_AT     = "CREATED_AT"
	COLUMN_UPDATED_AT     = "UPDATED_AT"
)

// Node 节点接口
type Node interface {
}

// CreateTableNode 创建表节点
type CreateTableNode struct {
	TableName  string           `xml:"tableName,attr"`
	Remarks    string           `xml:"remarks,attr"`
	Columns    []*ColumnNode    `xml:"column"`
	Attributes []*AttributeNode `xml:"attribute"`
}

type AttributeNode struct {
	Dialect string `xml:"dialect,attr"`
	Name    string `xml:"name,attr"`
	Value   string `xml:"value,attr"`
}

// ColumnNode 列节点
type ColumnNode struct {
	ColumnName         string               `xml:"columnName,attr"`
	DataType           string               `xml:"dataType,attr"`
	MaxLength          int                  `xml:"maxLength,attr"`
	NumericScale       int                  `xml:"numericScale,attr"`
	Nullable           bool                 `xml:"nullable,attr"`
	Unique             bool                 `xml:"unique,attr"`
	PrimaryKey         bool                 `xml:"primaryKey,attr"`
	KeyName            string               `xml:"keyName,attr"`
	DefaultValue       string               `xml:"defaultValue,attr"`
	DefaultOriginValue string               `xml:"defaultOriginValue,attr"`
	Remarks            string               `xml:"remarks,attr"`
	Dialects           []*ColumnDialectNode `xml:"columnDialect"`
}

// ColumnDialectNode 列方言节点
type ColumnDialectNode struct {
	Dialect            string `xml:"dialect,attr"`
	DataType           string `xml:"dataType,attr"`
	DefaultValue       string `xml:"defaultValue,attr"`
	DefaultOriginValue string `xml:"defaultOriginValue,attr"`
}

// CreateIndexNode 创建索引节点
type CreateIndexNode struct {
	TableName string             `xml:"tableName,attr"`
	IndexName string             `xml:"indexName,attr"`
	Unique    bool               `xml:"unique,attr"`
	Columns   []*IndexColumnNode `xml:"indexColumn"`
}

// IndexColumnNode 索引列节点
type IndexColumnNode struct {
	ColumnName string `xml:"columnName,attr"`
}

// CreatePrimaryKeyNode 创建主键节点
type CreatePrimaryKeyNode struct {
	TableName string           `xml:"tableName,attr"`
	KeyName   string           `xml:"keyName,attr"`
	Column    *IndexColumnNode `xml:"indexColumn"`
}

// DropTableNode 删除表节点
type DropTableNode struct {
	TableName string `xml:"tableName,attr"`
}

// DropIndexNode 删除索引节点
type DropIndexNode struct {
	TableName string `xml:"tableName,attr"`
	IndexName string `xml:"indexName,attr"`
}

// AddColumnNode 添加列节点
type AddColumnNode struct {
	TableName string        `xml:"tableName,attr"`
	Columns   []*ColumnNode `xml:"column"`
}

// RenameColumnNode 重命名列节点
type RenameColumnNode struct {
	TableName     string `xml:"tableName,attr"`
	ColumnName    string `xml:"columnName,attr"`
	NewColumnName string `xml:"newColumnName,attr"`
}

// AlterColumnNode 修改列节点
type AlterColumnNode struct {
	TableName  string      `xml:"tableName,attr"`
	ColumnName string      `xml:"columnName,attr"`
	Column     *ColumnNode `xml:"column"`
}

// DropColumnNode 删除列节点
type DropColumnNode struct {
	TableName  string `xml:"tableName,attr"`
	ColumnName string `xml:"columnName,attr"`
}

// DropPrimaryKeyNode 删除主键节点
type DropPrimaryKeyNode struct {
	TableName string `xml:"tableName,attr"`
}

// RenameTableNode 重命名表节点
type RenameTableNode struct {
	TableName    string `xml:"tableName,attr"`
	NewTableName string `xml:"newTableName,attr"`
}

// AlterTableRemarksNode 重命名表说明节点
type AlterTableRemarksNode struct {
	TableName string `xml:"tableName,attr"`
	Remarks   string `xml:"remarks,attr"`
}

// ScriptNode SQL脚本节点
type ScriptNode struct {
	Dialect string `xml:"dialect,attr"`
	Value   string `xml:",chardata"`
}

// Migratory SQL版本合并接口
type Migratory interface {
	// Name 合并器名称
	Name() string
	// InitChangeLogTable 初始化记录变更记录表
	InitChangeLogTable(context.Context, Driver, string) error
	// LastVersion 最后一次版本信息
	LastVersion(context.Context, Driver, string) (*version.Version, error)
	// NewChangeLog 创建一条新的表更记录
	NewChangeLog(context.Context, Driver, string, string) error
	// CompleteChangeLog 完成一条表更记录
	CompleteChangeLog(context.Context, Driver, string, string) error
	// CreateTable 创建表
	CreateTable(context.Context, Driver, *CreateTableNode) error
	// CreateIndex 创建索引
	CreateIndex(context.Context, Driver, *CreateIndexNode) error
	// CreatePrimaryKey 创建主键
	CreatePrimaryKey(context.Context, Driver, *CreatePrimaryKeyNode) error
	// DropTable 删除表
	DropTable(context.Context, Driver, *DropTableNode) error
	// DropIndex 删除索引
	DropIndex(context.Context, Driver, *DropIndexNode) error
	// AddColumn 添加列
	AddColumn(context.Context, Driver, *AddColumnNode) error
	// RenameColumn 重命名列
	RenameColumn(context.Context, Driver, *RenameColumnNode) error
	// AlterColumn 修改列
	AlterColumn(context.Context, Driver, *AlterColumnNode) error
	// DropColumn 删除列
	DropColumn(context.Context, Driver, *DropColumnNode) error
	// DropPrimaryKey 删除主键
	DropPrimaryKey(context.Context, Driver, *DropPrimaryKeyNode) error
	// RenameTable 重命名表
	RenameTable(context.Context, Driver, *RenameTableNode) error
	// AlterTableRemarks 修改表说明
	AlterTableRemarks(context.Context, Driver, *AlterTableRemarksNode) error
	// Script 执行自定义SQL脚本
	Script(context.Context, Driver, *ScriptNode) error
	// SetQuotePolicy 设置引号策略
	SetQuotePolicy(quotePolicy QuotePolicy)
}

type DefaultMigratory struct {
	name            string
	showTablesSql   string
	dataTypeMapper  map[string]string
	quoter          *Quoter
	overwriteQuoter *Quoter
	reservedWords   map[string]struct{}
}

func NewDefaultMigratory(name, showTablesSql string, dataTypeMapper map[string]string, quoter *Quoter, reservedWords ...string) DefaultMigratory {
	if dataTypeMapper == nil {
		dataTypeMapper = make(map[string]string)
	}
	reservedWordsMap := map[string]struct{}{}
	for _, reservedWord := range reservedWords {
		reservedWordsMap[strings.ToUpper(reservedWord)] = struct{}{}
	}
	return DefaultMigratory{
		name:           name,
		showTablesSql:  showTablesSql,
		dataTypeMapper: dataTypeMapper,
		quoter:         quoter,
		reservedWords:  reservedWordsMap,
	}
}

func (m *DefaultMigratory) Name() string {
	return m.name
}

// InitChangeLogTable 初始化变更记录表
func (m *DefaultMigratory) InitChangeLogTable(ctx context.Context, driver Driver, changeTableName string) error {
	if m.showTablesSql == "" {
		return errors.New("showTablesSql must not be empty")
	}
	rows, err := driver.Query(ctx, m.showTablesSql)
	exists := false
	if exists, err = m.ExistsTable(changeTableName, rows, err); err != nil || exists {
		return err
	}
	return m.CreateChangeTable(ctx, driver, changeTableName)
}

func (m *DefaultMigratory) ExistsTable(changeTableName string, rows Rows, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	changeTableName = strings.ToLower(changeTableName)
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			return false, err
		}
		if changeTableName == strings.ToLower(tableName) {
			return true, nil
		}
	}
	return false, nil
}

func (m *DefaultMigratory) quote(s string) string {
	r, err := m.Quoter().Quote(s)
	if err != nil {
		panic(err)
	}
	return r
}

func (m *DefaultMigratory) quoteTo(buf *strings.Builder, value string) {
	if err := m.Quoter().QuoteTo(buf, value); err != nil {
		panic(err)
	}
}

func (m *DefaultMigratory) joinWrite(buf *strings.Builder, a []string) {
	if err := m.Quoter().JoinWrite(buf, a, ", "); err != nil {
		panic(err)
	}
}

func (m *DefaultMigratory) CreateChangeTable(ctx context.Context, driver Driver, changeTableName string) error {
	sql := fmt.Sprintf("CREATE TABLE %s(%s %s PRIMARY KEY, %s %s(255) NOT NULL, %s %s DEFAULT 0 NOT NULL, %s %s, %s %s)",
		m.quote(changeTableName),
		m.quote(COLUMN_ID), m.dataTypeMapper[Bigint],
		m.quote(COLUMN_CHANGE_VERSION), m.dataTypeMapper[Varchar],
		m.quote(COLUMN_IS_SUCCESS), m.dataTypeMapper[Boolean],
		m.quote(COLUMN_CREATED_AT), m.dataTypeMapper[Timestamp],
		m.quote(COLUMN_UPDATED_AT), m.dataTypeMapper[Timestamp],
	)
	return driver.Execute(ctx, sql)
}

func (m *DefaultMigratory) LastVersion(ctx context.Context, driver Driver, changeTableName string) (*version.Version, error) {
	rows, err := driver.Query(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s = 1", m.quote(COLUMN_CHANGE_VERSION), m.quote(changeTableName), m.quote(COLUMN_IS_SUCCESS)))
	if err != nil {
		return nil, err
	}
	var versions []*version.Version
	defer rows.Close()
	for rows.Next() {
		var changeVersion string
		if err = rows.Scan(&changeVersion); err != nil {
			return nil, err
		}
		ver, err := version.NewVersion(changeVersion)
		if err != nil {
			return nil, err
		}
		versions = append(versions, ver)
	}
	if len(versions) == 0 {
		return nil, nil
	}
	sort.Sort(version.Collection(versions))
	return versions[len(versions)-1], nil
}

func (m *DefaultMigratory) NewChangeLog(ctx context.Context, driver Driver, changeTableName, version string) error {
	return driver.Execute(ctx,
		fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s) VALUES(?, 0, ?, ?)",
			m.quote(changeTableName), m.quote(COLUMN_CHANGE_VERSION), m.quote(COLUMN_IS_SUCCESS), m.quote(COLUMN_CREATED_AT), m.quote(COLUMN_UPDATED_AT)),
		version, time.Now(), time.Now())
}

func (m *DefaultMigratory) CompleteChangeLog(ctx context.Context, driver Driver, changeTableName, version string) error {
	return driver.Execute(ctx,
		fmt.Sprintf("UPDATE %s SET %s = 1, %s = ? WHERE %s = ? AND %s = 0",
			m.quote(changeTableName), m.quote(COLUMN_IS_SUCCESS), m.quote(COLUMN_UPDATED_AT), m.quote(COLUMN_CHANGE_VERSION), m.quote(COLUMN_IS_SUCCESS)),
		time.Now(), version)
}

func (m *DefaultMigratory) CreateTable(ctx context.Context, driver Driver, node *CreateTableNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE TABLE ")
	m.quoteTo(&builder, node.TableName)
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
		m.quoteTo(&builder, pkColumn.KeyName)
		builder.WriteString(" PRIMARY KEY (")
		m.quoteTo(&builder, pkColumn.ColumnName)
		builder.WriteString(")")
	}
	builder.WriteString("\n)")
	if err := driver.Execute(ctx, builder.String()); err != nil {
		return err
	}

	if node.Remarks != "" {
		if err := driver.Execute(ctx, fmt.Sprintf("COMMENT ON TABLE %s IS '%s'",
			m.quote(node.TableName), ReplaceRemarks(node.Remarks))); err != nil {
			return err
		}
	}
	for _, column := range node.Columns {
		if column.Remarks != "" {
			if err := driver.Execute(ctx, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
				m.quote(node.TableName), m.quote(column.ColumnName), ReplaceRemarks(column.Remarks))); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *DefaultMigratory) CreateTableColumn(node *ColumnNode, builder *strings.Builder) bool {
	var dialectNode *ColumnDialectNode

	// 查找方言
	for _, dialect := range node.Dialects {
		if dialect.Dialect == m.Name() {
			dialectNode = dialect
			break
		}
	}
	m.quoteTo(builder, node.ColumnName)
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
		builder.WriteString(ColumnType(node.DataType, m.dataTypeMapper[node.DataType], node.MaxLength, node.NumericScale))
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

func (m *DefaultMigratory) CreateIndex(ctx context.Context, driver Driver, node *CreateIndexNode) error {
	var builder strings.Builder
	builder.WriteString("CREATE")
	if node.Unique {
		builder.WriteString(" UNIQUE")
	}
	builder.WriteString(" INDEX ")
	m.quoteTo(&builder, node.IndexName)
	builder.WriteString(" ON ")
	m.quoteTo(&builder, node.TableName)
	builder.WriteString(" (")
	var columns []string
	for _, columnNode := range node.Columns {
		columns = append(columns, columnNode.ColumnName)
	}
	m.joinWrite(&builder, columns)
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) CreatePrimaryKey(ctx context.Context, driver Driver, node *CreatePrimaryKeyNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.quoteTo(&builder, node.TableName)
	builder.WriteString(" ADD CONSTRAINT ")
	m.quoteTo(&builder, node.KeyName)
	builder.WriteString(" PRIMARY KEY (")
	m.quoteTo(&builder, node.Column.ColumnName)
	builder.WriteString(")")
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) DropTable(ctx context.Context, driver Driver, node *DropTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP TABLE %s", m.quote(node.TableName)))
}

func (m *DefaultMigratory) DropIndex(ctx context.Context, driver Driver, node *DropIndexNode) error {
	return driver.Execute(ctx, fmt.Sprintf("DROP INDEX %s", m.quote(node.IndexName)))
}

func (m *DefaultMigratory) AddColumn(ctx context.Context, driver Driver, node *AddColumnNode) error {
	for _, column := range node.Columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		m.quoteTo(&builder, node.TableName)
		builder.WriteString(" ADD ")
		if pk := m.CreateTableColumn(column, &builder); pk {
			return errors.New("adding columns is not allowed as a primary key")
		}
		if err := driver.Execute(ctx, builder.String()); err != nil {
			return err
		}
		if column.Remarks != "" {
			if err := driver.Execute(ctx, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
				m.quote(node.TableName), m.quote(column.ColumnName), ReplaceRemarks(column.Remarks))); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *DefaultMigratory) RenameColumn(ctx context.Context, driver Driver, node *RenameColumnNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.quoteTo(&builder, node.TableName)
	builder.WriteString(" RENAME COLUMN ")
	m.quoteTo(&builder, node.ColumnName)
	builder.WriteString(" TO ")
	m.quoteTo(&builder, node.NewColumnName)
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) AlterColumn(ctx context.Context, driver Driver, node *AlterColumnNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	m.quoteTo(&builder, node.TableName)
	builder.WriteString(" MODIFY ")
	node.Column.ColumnName = node.ColumnName
	if pk := m.CreateTableColumn(node.Column, &builder); pk {
		return errors.New("alter columns is not allowed as a primary key")
	}
	return driver.Execute(ctx, builder.String())
}

func (m *DefaultMigratory) DropColumn(ctx context.Context, driver Driver, node *DropColumnNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", m.quote(node.TableName), m.quote(node.ColumnName)))
}

func (m *DefaultMigratory) DropPrimaryKey(ctx context.Context, driver Driver, node *DropPrimaryKeyNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", m.quote(node.TableName)))
}

func (m *DefaultMigratory) RenameTable(ctx context.Context, driver Driver, node *RenameTableNode) error {
	return driver.Execute(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", m.quote(node.TableName), m.quote(node.NewTableName)))
}

func (m *DefaultMigratory) AlterTableRemarks(ctx context.Context, driver Driver, node *AlterTableRemarksNode) error {
	return driver.Execute(ctx, fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", m.quote(node.TableName), ReplaceRemarks(node.Remarks)))
}

func (m *DefaultMigratory) Script(ctx context.Context, driver Driver, node *ScriptNode) error {
	if node.Value == "" || (node.Dialect != m.name && node.Dialect != allDialect) {
		return nil
	}
	for _, statement := range SplitSQLStatements(node.Value) {
		if err := driver.Execute(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (m *DefaultMigratory) Quoter() *Quoter {
	if m.overwriteQuoter != nil {
		return m.overwriteQuoter
	}
	return m.defaultQuoter()
}

func (m *DefaultMigratory) defaultQuoter() *Quoter {
	if m.quoter == nil {
		return CommonQuoter
	}
	return m.quoter
}

func (m *DefaultMigratory) SetQuoter(quoter *Quoter) {
	m.overwriteQuoter = quoter
}

func (m *DefaultMigratory) RegisterReservedWords(words ...string) {
	for _, word := range words {
		if word == "" {
			continue
		}
		m.reservedWords[strings.ToUpper(word)] = struct{}{}
	}
}

func (m *DefaultMigratory) RegisterDataType(name, value string) {
	m.dataTypeMapper[name] = value
}

func (m *DefaultMigratory) ShowTablesSql() string {
	return m.showTablesSql
}

func (m *DefaultMigratory) SetQuotePolicy(quotePolicy QuotePolicy) {
	switch quotePolicy {
	case QuotePolicyNone:
		m.SetQuoter(m.defaultQuoter().Clone(WithIsReserved(AlwaysNoReserve)))
	case QuotePolicyReserved:
		m.SetQuoter(m.defaultQuoter().Clone(WithIsReserved(m.IsReserved)))
	case QuotePolicyAlways:
		fallthrough
	default:
		m.SetQuoter(m.defaultQuoter())
	}
}

func (m *DefaultMigratory) IsReserved(name string) bool {
	_, ok := m.reservedWords[strings.ToUpper(name)]
	return ok
}
