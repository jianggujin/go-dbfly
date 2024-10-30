package dbfly

import (
	"fmt"
	"github.com/hashicorp/go-version"
	"sort"
	"strings"
	"time"
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
	driver          Driver
	changeTableName string
	inited          bool
}

// 创建一个Mysql合并实现实例
func NewMysqlMigratory(driver Driver) Migratory {
	return &MysqlMigratory{
		driver:          driver,
		changeTableName: changeTableName,
	}
}

func (m *MysqlMigratory) SetChangeTableName(changeTableName string) {
	m.changeTableName = changeTableName
}

func (m *MysqlMigratory) Name() string {
	return "mysql"
}

// 初始化变更记录表
func (m *MysqlMigratory) initChangeLogTable() error {
	if m.inited {
		return nil
	}
	rows, err := m.driver.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		if m.changeTableName == strings.ToLower(tableName) {
			m.inited = true
			return nil
		}
	}
	err = m.driver.Execute("CREATE TABLE " + m.changeTableName + "(id BIGINT PRIMARY KEY AUTO_INCREMENT, change_version VARCHAR(255) NOT NULL, is_success TINYINT DEFAULT 0 NOT NULL, created_at DATETIME, updated_at DATETIME) ENGINE = InnoDB")
	if err != nil {
		return err
	}
	m.inited = true
	return nil
}

func (m *MysqlMigratory) LastVersion() (*version.Version, error) {
	if err := m.initChangeLogTable(); err != nil {
		return nil, err
	}
	rows, err := m.driver.Query("SELECT change_version FROM " + m.changeTableName + " WHERE is_success = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []*version.Version
	for rows.Next() {
		var changeVersion string
		if err := rows.Scan(&changeVersion); err != nil {
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

func (m *MysqlMigratory) Migrate(nodes []Node, version *version.Version) error {
	if err := m.initChangeLogTable(); err != nil {
		return err
	}
	err := m.driver.Execute("INSERT INTO "+m.changeTableName+"(change_version, is_success, created_at, updated_at) VALUES(?, 0, ?, ?)", version.Original(), time.Now(), time.Now())
	if err != nil {
		return err
	}
	for _, node := range nodes {
		switch n := node.(type) {
		case *CreateTableNode:
			err = m.createTable(n)
		case *CreateIndexNode:
			err = m.createIndex(n)
		case *CreatePrimaryKeyNode:
			err = m.createPrimaryKey(n)
		case *DropTableNode:
			err = m.dropTable(n)
		case *DropIndexNode:
			err = m.dropIndex(n)
		case *AddColumnNode:
			err = m.addColumn(n)
		case *AlterColumnNode:
			err = m.alterColumn(n)
		case *DropColumnNode:
			err = m.dropColumn(n)
		case *DropPrimaryKeyNode:
			err = m.dropPrimaryKey(n)
		case *RenameTableNode:
			err = m.renameTable(n)
		case *AlterTableRemarksNode:
			err = m.alterTableRemarks(n)
		case *ScriptNode:
			err = m.script(n)
		}
		if err != nil {
			return err
		}
	}
	return m.driver.Execute("UPDATE "+m.changeTableName+" SET is_success = 1, updated_at = ? WHERE change_version = ? AND is_success = 0", time.Now(), version.Original())
}

func (m *MysqlMigratory) createTable(node *CreateTableNode) error {
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
	return m.driver.Execute(builder.String())
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

func (m *MysqlMigratory) createIndex(node *CreateIndexNode) error {
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
	return m.driver.Execute(builder.String())
}

func (m *MysqlMigratory) createPrimaryKey(node *CreatePrimaryKeyNode) error {
	var builder strings.Builder
	builder.WriteString("ALTER TABLE ")
	builder.WriteString(node.TableName)
	builder.WriteString(" ADD CONSTRAINT ")
	builder.WriteString(node.KeyName)
	builder.WriteString(" PRIMARY KEY (")
	builder.WriteString(node.Column.ColumnName)
	builder.WriteString(")")
	return m.driver.Execute(builder.String())
}

func (m *MysqlMigratory) dropTable(node *DropTableNode) error {
	return m.driver.Execute(fmt.Sprintf("DROP TABLE %s", node.TableName))
}

func (m *MysqlMigratory) dropIndex(node *DropIndexNode) error {
	return m.driver.Execute(fmt.Sprintf("DROP INDEX %s ON %s", node.IndexName, node.TableName))
}

func (m *MysqlMigratory) addColumn(node *AddColumnNode) error {
	for _, column := range node.Columns {
		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		builder.WriteString(node.TableName)
		builder.WriteString(" ADD ")
		m.createTableColumn(column, &builder)
		if err := m.driver.Execute(builder.String()); err != nil {
			return err
		}
	}
	return nil
}

func (m *MysqlMigratory) alterColumn(node *AlterColumnNode) error {
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
	return m.driver.Execute(builder.String())
}

func (m *MysqlMigratory) dropColumn(node *DropColumnNode) error {
	return m.driver.Execute(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", node.TableName, node.ColumnName))
}

func (m *MysqlMigratory) dropPrimaryKey(node *DropPrimaryKeyNode) error {
	return m.driver.Execute(fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", node.TableName))
}

func (m *MysqlMigratory) renameTable(node *RenameTableNode) error {
	return m.driver.Execute(fmt.Sprintf("RENAME TABLE %s TO %s", node.TableName, node.NewTableName))
}

func (m *MysqlMigratory) alterTableRemarks(node *AlterTableRemarksNode) error {
	return m.driver.Execute(fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", node.TableName, strings.ReplaceAll(node.Remarks, "'", "''")))
}

func (m *MysqlMigratory) script(node *ScriptNode) error {
	if (node.Dialect != "" && node.Dialect != m.Name()) || node.Value == "" {
		return nil
	}
	for _, statement := range splitSQLStatements(node.Value) {
		if err := m.driver.Execute(statement); err != nil {
			return err
		}
	}
	return nil
}
