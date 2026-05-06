package dbfly

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
)

type DDL interface {
	Execute(context.Context, *Dbfly) error
}

type Condition interface {
	Check(context.Context, *Dbfly) (bool, error)
}

type ChangeSets []ChangeSet

type ChangeSet struct {
	Id       string
	Author   string
	OnFail   string
	Filename string
	DDLs     []DDL
}

type ConditionsNode struct {
	Conditions []*ConditionNode `xml:"condition"`
}

func (n *ConditionsNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	if n == nil || len(n.Conditions) == 0 {
		return true, nil
	}
	for _, node := range n.Conditions {
		ok, err := node.Check(ctx, fly)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

type ConditionNode struct {
	Conditions []Condition
}

func (n *ConditionNode) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch ele := token.(type) {
		case xml.StartElement:
			name := ele.Name.Local
			var condition Condition
			switch name {
			case "tableExists":
				condition = &TableExistsNode{}
			case "columnExists":
				condition = &ColumnExistsNode{}
			case "primaryKeyExists":
				condition = &PrimaryKeyExistsNode{}
			case "indexExists":
				condition = &IndexExistsNode{}
			case "rowCount":
				condition = &RowCountNode{}
			case "sqlCheck":
				condition = &SqlCheckNode{}
			case "dbms":
				condition = &DbmsNode{}
			default:
				return New("invalid child element <%s> inside condition element", ele.Name.Local)
			}
			if err = decoder.DecodeElement(condition, &ele); err != nil {
				return err
			}
			n.Conditions = append(n.Conditions, condition)
		}
	}
	return nil
}

func (n *ConditionNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	if n == nil || len(n.Conditions) == 0 {
		return true, nil
	}
	for _, condition := range n.Conditions {
		ok, err := condition.Check(ctx, fly)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

type TableExistsNode struct {
	TableName string `xml:"tableName,attr"`
	Not       bool   `xml:"not,attr"`
}

func (n *TableExistsNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	migratory := fly.Migratory()
	pass, _, err := migratory.MetaData().ExistsTable(ctx, fly.Driver(), n.TableName)
	if err != nil {
		return false, err
	}
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type ColumnExistsNode struct {
	TableName  string `xml:"tableName,attr"`
	ColumnName string `xml:"columnName,attr"`
	Not        bool   `xml:"not,attr"`
}

func (n *ColumnExistsNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	migratory := fly.Migratory()
	pass, _, _, err := migratory.MetaData().ExistsColumn(ctx, fly.Driver(), n.TableName, n.ColumnName)
	if err != nil {
		return false, err
	}
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type PrimaryKeyExistsNode struct {
	TableName string `xml:"tableName,attr"`
	Not       bool   `xml:"not,attr"`
}

func (n *PrimaryKeyExistsNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	migratory := fly.Migratory()
	pass, _, err := migratory.MetaData().ExistsPrimaryKey(ctx, fly.Driver(), n.TableName)
	if err != nil {
		return false, err
	}
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type IndexExistsNode struct {
	TableName string `xml:"tableName,attr"`
	IndexName string `xml:"indexName,attr"`
	Not       bool   `xml:"not,attr"`
}

func (n *IndexExistsNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	migratory := fly.Migratory()
	pass, _, _, err := migratory.MetaData().ExistsIndex(ctx, fly.Driver(), n.TableName, n.IndexName)
	if err != nil {
		return false, err
	}
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type RowCountNode struct {
	TableName    string `xml:"tableName,attr"`
	ExpectedRows int    `xml:"expectedRows,attr"`
	Not          bool   `xml:"not,attr"`
}

func (n *RowCountNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	migratory := fly.Migratory()
	sql := fmt.Sprintf(`SELECT count(*) FROM %s`, migratory.MetaData().Quoter().MustQuote(n.TableName))
	count, err := doGetScalar[int](ctx, fly.Driver(), sql)
	if err != nil {
		return false, err
	}
	pass := count == n.ExpectedRows
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type SqlCheckNode struct {
	Sql            *SqlNode `xml:"sql"`
	ExpectedResult string   `xml:"expectedResult,attr"`
	Not            bool     `xml:"not,attr"`
}

func (n *SqlCheckNode) Check(ctx context.Context, fly *Dbfly) (bool, error) {
	sql := n.Sql.Content
	sql = strings.TrimSpace(sql)
	var pass bool
	if sql == "" {
		pass = true
	} else {
		value, err := doGetScalar[string](ctx, fly.Driver(), sql)
		if err != nil {
			return false, err
		}
		pass = value == n.ExpectedResult
	}
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

type DbmsNode struct {
	Name string `xml:"name,attr"`
	Not  bool   `xml:"not,attr"`
}

func (n *DbmsNode) Check(_ context.Context, fly *Dbfly) (bool, error) {
	pass := fly.Migratory().MetaData().Dbms() == n.Name
	if n.Not {
		pass = !pass
	}
	return pass, nil
}

// CreateTableNode 创建表节点
type CreateTableNode struct {
	TableName  string          `xml:"tableName,attr"`
	Comment    string          `xml:"comment,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Columns    []*ColumnNode   `xml:"column"`
	Attributes *AttributesNode `xml:"dbmsAttributes"`
}

func (n *CreateTableNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().CreateTable(ctx, fly.Driver(), n.TableName, n.Comment, n.Columns, n.Attributes)
}

// ColumnNode 列节点
type ColumnNode struct {
	ColumnName         string            `xml:"columnName,attr"`
	DataType           string            `xml:"dataType,attr"`
	MaxLength          int               `xml:"maxLength,attr"`
	NumericScale       int               `xml:"numericScale,attr"`
	Nullable           bool              `xml:"nullable,attr"`
	Unique             bool              `xml:"unique,attr"`
	PrimaryKey         bool              `xml:"primaryKey,attr"`
	KeyName            string            `xml:"keyName,attr"`
	DefaultValue       string            `xml:"defaultValue,attr"`
	DefaultOriginValue string            `xml:"defaultOriginValue,attr"`
	Comment            string            `xml:"comment,attr"`
	ColumnDbms         []*ColumnDbmsNode `xml:"columnDbms"`
}

type ColumnDbmsNode struct {
	Dbms               string `xml:"dbms,attr"`
	DataType           string `xml:"dataType,attr"`
	DefaultValue       string `xml:"defaultValue,attr"`
	DefaultOriginValue string `xml:"defaultOriginValue,attr"`
}

type AttributesNode struct {
	Attributes []*AttributeNode `xml:"attribute"`
}

type AttributeNode struct {
	Dbms  string `xml:"dbms,attr"`
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// CreateIndexNode 创建索引节点
type CreateIndexNode struct {
	TableName  string             `xml:"tableName,attr"`
	IndexName  string             `xml:"indexName,attr"`
	Unique     bool               `xml:"unique,attr"`
	Conditions *ConditionsNode    `xml:"conditions"`
	Columns    []*IndexColumnNode `xml:"column"`
	Attributes *AttributesNode    `xml:"attributes"`
}

func (n *CreateIndexNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().CreateIndex(ctx, fly.Driver(), n.TableName, n.IndexName, n.Unique, n.Columns, n.Attributes)
}

type IndexColumnNode struct {
	Name string `xml:"name,attr"`
}

// CreatePrimaryKeyNode 创建主键节点
type CreatePrimaryKeyNode struct {
	TableName  string             `xml:"tableName,attr"`
	KeyName    string             `xml:"keyName,attr"`
	Conditions *ConditionsNode    `xml:"conditions"`
	Columns    []*IndexColumnNode `xml:"column"`
	Attributes *AttributesNode    `xml:"attributes"`
}

func (n *CreatePrimaryKeyNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().CreatePrimaryKey(ctx, fly.Driver(), n.TableName, n.KeyName, n.Columns, n.Attributes)
}

// DropTableNode 删除表节点
type DropTableNode struct {
	TableName  string          `xml:"tableName,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Attributes *AttributesNode `xml:"attributes"`
}

func (n *DropTableNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().DropTable(ctx, fly.Driver(), n.TableName, n.Attributes)
}

// DropIndexNode 删除索引节点
type DropIndexNode struct {
	TableName  string          `xml:"tableName,attr"`
	IndexName  string          `xml:"indexName,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Attributes *AttributesNode `xml:"attributes"`
}

func (n *DropIndexNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().DropIndex(ctx, fly.Driver(), n.TableName, n.IndexName, n.Attributes)
}

// AddColumnNode 添加列节点
type AddColumnNode struct {
	TableName  string                 `xml:"tableName,attr"`
	Conditions *ConditionsNode        `xml:"conditions"`
	Columns    []*AddColumnColumnNode `xml:"column"`
	Attributes *AttributesNode        `xml:"attributes"`
}

func (n *AddColumnNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().AddColumn(ctx, fly.Driver(), n.TableName, n.Columns, n.Attributes)
}

type AddColumnColumnNode struct {
	ColumnName         string            `xml:"columnName,attr"`
	DataType           string            `xml:"dataType,attr"`
	MaxLength          int               `xml:"maxLength,attr"`
	NumericScale       int               `xml:"numericScale,attr"`
	Nullable           bool              `xml:"nullable,attr"`
	Unique             bool              `xml:"unique,attr"`
	DefaultValue       string            `xml:"defaultValue,attr"`
	DefaultOriginValue string            `xml:"defaultOriginValue,attr"`
	Comment            string            `xml:"comment,attr"`
	ColumnDbms         []*ColumnDbmsNode `xml:"columnDbms"`
}

// RenameColumnNode 重命名列节点
type RenameColumnNode struct {
	TableName     string          `xml:"tableName,attr"`
	ColumnName    string          `xml:"columnName,attr"`
	NewColumnName string          `xml:"newColumnName,attr"`
	Conditions    *ConditionsNode `xml:"conditions"`
	Attributes    *AttributesNode `xml:"attributes"`
}

func (n *RenameColumnNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().RenameColumn(ctx, fly.Driver(), n.TableName, n.ColumnName, n.NewColumnName, n.Attributes)
}

// AlterColumnNode 修改列节点
type AlterColumnNode struct {
	TableName  string                 `xml:"tableName,attr"`
	ColumnName string                 `xml:"columnName,attr"`
	Conditions *ConditionsNode        `xml:"conditions"`
	Column     *AlterColumnColumnNode `xml:"column"`
	Attributes *AttributesNode        `xml:"attributes"`
}

func (n *AlterColumnNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().AlterColumn(ctx, fly.Driver(), n.TableName, n.ColumnName, n.Column, n.Attributes)
}

type AlterColumnColumnNode struct {
	DataType           string            `xml:"dataType,attr"`
	MaxLength          int               `xml:"maxLength,attr"`
	NumericScale       int               `xml:"numericScale,attr"`
	Nullable           bool              `xml:"nullable,attr"`
	Unique             bool              `xml:"unique,attr"`
	DefaultValue       string            `xml:"defaultValue,attr"`
	DefaultOriginValue string            `xml:"defaultOriginValue,attr"`
	Comment            string            `xml:"comment,attr"`
	ColumnDbms         []*ColumnDbmsNode `xml:"columnDbms"`
}

// DropColumnNode 删除列节点
type DropColumnNode struct {
	TableName  string          `xml:"tableName,attr"`
	ColumnName string          `xml:"columnName,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Attributes *AttributesNode `xml:"attributes"`
}

func (n *DropColumnNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().DropColumn(ctx, fly.Driver(), n.TableName, n.ColumnName, n.Attributes)
}

// DropPrimaryKeyNode 删除主键节点
type DropPrimaryKeyNode struct {
	TableName  string          `xml:"tableName,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Attributes *AttributesNode `xml:"attributes"`
}

func (n *DropPrimaryKeyNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().DropPrimaryKey(ctx, fly.Driver(), n.TableName, n.Attributes)
}

// RenameTableNode 重命名表节点
type RenameTableNode struct {
	TableName    string          `xml:"tableName,attr"`
	NewTableName string          `xml:"newTableName,attr"`
	Conditions   *ConditionsNode `xml:"conditions"`
	Attributes   *AttributesNode `xml:"attributes"`
}

func (n *RenameTableNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().RenameTable(ctx, fly.Driver(), n.TableName, n.NewTableName, n.Attributes)
}

// AlterTableCommentNode 重命名表说明节点
type AlterTableCommentNode struct {
	TableName  string          `xml:"tableName,attr"`
	Comment    string          `xml:"comment,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Attributes *AttributesNode `xml:"attributes"`
}

func (n *AlterTableCommentNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	return fly.Migratory().AlterTableComment(ctx, fly.Driver(), n.TableName, n.Comment, n.Attributes)
}

// SqlFileNode SQL脚本节点
type SqlFileNode struct {
	Conditions  *ConditionsNode    `xml:"conditions"`
	Path        string             `xml:"path,attr"`
	SqlFileDbms []*SqlFileDbmsNode `xml:"sqlFileDbms"`
}

// SqlFileDbmsNode SQL脚本方言文件节点
type SqlFileDbmsNode struct {
	Dbms string `xml:"dbms,attr"`
	Path string `xml:"path,attr"`
}

func (n *SqlFileNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	path := n.Path
	for _, dbmsNode := range n.SqlFileDbms {
		if dbmsNode.Dbms == fly.Migratory().MetaData().Dbms() {
			path = dbmsNode.Path
			break
		}
	}
	content, err := fly.Source().Read(path)
	if err != nil {
		return err
	}
	return fly.Migratory().Script(ctx, fly.Driver(), string(content))
}

type SqlNode struct {
	Content string `xml:",chardata"`
}

// ChangeSetNode 变更集节点
type ChangeSetNode struct {
	Id         string
	Author     string
	OnFail     string
	Conditions *ConditionsNode
	DDLs       []DDL
}

func (n *ChangeSetNode) Execute(ctx context.Context, fly *Dbfly) error {
	for _, ddl := range n.DDLs {
		if err := ddl.Execute(ctx, fly); err != nil {
			return err
		}
	}
	return nil
}

func (n *ChangeSetNode) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	// 先解析属性
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "id":
			n.Id = attr.Value
		case "author":
			n.Author = attr.Value
		case "onFail":
			n.OnFail = attr.Value
		}
	}
	// 然后手动解析子元素
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch ele := token.(type) {
		case xml.StartElement:
			name := ele.Name.Local
			var ddl DDL
			switch name {
			case "createTable":
				ddl = &CreateTableNode{}
			case "createIndex":
				ddl = &CreateIndexNode{}
			case "createPrimaryKey":
				ddl = &CreatePrimaryKeyNode{}
			case "dropTable":
				ddl = &DropTableNode{}
			case "dropIndex":
				ddl = &DropIndexNode{}
			case "addColumn":
				ddl = &AddColumnNode{}
			case "renameColumn":
				ddl = &RenameColumnNode{}
			case "alterColumn":
				ddl = &AlterColumnNode{}
			case "dropColumn":
				ddl = &DropColumnNode{}
			case "dropPrimaryKey":
				ddl = &DropPrimaryKeyNode{}
			case "renameTable":
				ddl = &RenameTableNode{}
			case "alterTableComment":
				ddl = &AlterTableCommentNode{}
			case "sqlFile":
				ddl = &SqlFileNode{}
			case "insert":
				ddl = &InsertNode{}
			case "update":
				ddl = &UpdateNode{}
			case "delete":
				ddl = &DeleteNode{}
			case "sqlInline":
				ddl = &SqlInlineNode{}
			case "transaction":
				ddl = &TransactionNode{}
			case "conditions":
				n.Conditions = &ConditionsNode{}
				if err = decoder.DecodeElement(n.Conditions, &ele); err != nil {
					return err
				}
				continue
			default:
				return New("invalid DDL element <%s>", name)
			}
			if err = decoder.DecodeElement(ddl, &ele); err != nil {
				return err
			}
			n.DDLs = append(n.DDLs, ddl)
		case xml.EndElement:
			if ele.Name.Local == start.Name.Local {
				return nil
			}
		}
	}
	return nil
}

// DataColumnNode DML列节点
type DataColumnNode struct {
	Name        string `xml:"name,attr"`
	Value       string `xml:"value,attr"`
	OriginValue string `xml:"originValue,attr"`
}

// DataRowNode DML行节点（批量插入）
type DataRowNode struct {
	Columns []*DataColumnNode `xml:"column"`
}

// InsertNode 插入数据节点
type InsertNode struct {
	TableName  string            `xml:"tableName,attr"`
	Conditions *ConditionsNode   `xml:"conditions"`
	Columns    []*DataColumnNode `xml:"column"` // 单行模式
	Rows       []*DataRowNode    `xml:"row"`    // 批量模式
}

func (n *InsertNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	quoter := fly.Migratory().MetaData().Quoter()
	var sql string

	if len(n.Rows) > 0 {
		// 批量模式
		columns := n.Rows[0].Columns
		var builder strings.Builder
		builder.WriteString("INSERT INTO ")
		quoter.MustQuoteTo(&builder, n.TableName)
		builder.WriteString(" (")
		for i, col := range columns {
			if i > 0 {
				builder.WriteString(", ")
			}
			quoter.MustQuoteTo(&builder, col.Name)
		}
		builder.WriteString(") VALUES ")
		for ri, row := range n.Rows {
			if ri > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString("(")
			for i, col := range row.Columns {
				if i > 0 {
					builder.WriteString(", ")
				}
				writeColumnValue(&builder, col)
			}
			builder.WriteString(")")
		}
		sql = builder.String()
		fly.logger.Debug("insert", "tableName", n.TableName, "rows", len(n.Rows))
		return fly.Execute(ctx, sql)
	}

	// 单行模式
	if len(n.Columns) == 0 {
		return nil
	}
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	quoter.MustQuoteTo(&builder, n.TableName)
	builder.WriteString(" (")
	for i, col := range n.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		quoter.MustQuoteTo(&builder, col.Name)
	}
	builder.WriteString(") VALUES (")
	for i, col := range n.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		writeColumnValue(&builder, col)
	}
	builder.WriteString(")")
	sql = builder.String()
	fly.logger.Debug("insert", "tableName", n.TableName, "rows", 1)
	return fly.Execute(ctx, sql)
}

// UpdateNode 更新数据节点
type UpdateNode struct {
	TableName  string            `xml:"tableName,attr"`
	Conditions *ConditionsNode   `xml:"conditions"`
	Columns    []*DataColumnNode `xml:"column"`
	Where      string            `xml:"where"`
}

func (n *UpdateNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	quoter := fly.Migratory().MetaData().Quoter()
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	quoter.MustQuoteTo(&builder, n.TableName)
	builder.WriteString(" SET ")
	for i, col := range n.Columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		quoter.MustQuoteTo(&builder, col.Name)
		builder.WriteString(" = ")
		writeColumnValue(&builder, col)
	}
	if n.Where != "" {
		builder.WriteString(" WHERE ")
		builder.WriteString(n.Where)
	}
	sql := builder.String()
	fly.logger.Debug("update", "tableName", n.TableName)
	return fly.Execute(ctx, sql)
}

// DeleteNode 删除数据节点
type DeleteNode struct {
	TableName  string          `xml:"tableName,attr"`
	Conditions *ConditionsNode `xml:"conditions"`
	Where      string          `xml:"where"`
}

func (n *DeleteNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	quoter := fly.Migratory().MetaData().Quoter()
	var builder strings.Builder
	builder.WriteString("DELETE FROM ")
	quoter.MustQuoteTo(&builder, n.TableName)
	if n.Where != "" {
		builder.WriteString(" WHERE ")
		builder.WriteString(n.Where)
	}
	sql := builder.String()
	fly.logger.Debug("delete", "tableName", n.TableName)
	return fly.Execute(ctx, sql)
}

// SqlInlineNode 内联SQL节点
type SqlInlineNode struct {
	Conditions *ConditionsNode `xml:"conditions"`
	Default    string          `xml:"default"`
	SqlDbms    []*SqlDbmsNode  `xml:"sqlDbms"`
}

func (n *SqlInlineNode) Execute(ctx context.Context, fly *Dbfly) error {
	if ok, err := n.Conditions.Check(ctx, fly); !ok || err != nil {
		return err
	}
	content := n.Default
	for _, dbmsNode := range n.SqlDbms {
		if dbmsNode.Dbms == fly.Migratory().MetaData().Dbms() {
			content = dbmsNode.Content
			break
		}
	}
	fly.logger.Debug("sqlInline")
	return fly.Migratory().Script(ctx, fly.Driver(), content)
}

// SqlDbmsNode SQL方言节点
type SqlDbmsNode struct {
	Dbms    string `xml:"dbms,attr"`
	Content string `xml:",chardata"`
}

// TransactionNode 事务控制节点
type TransactionNode struct {
	DMLs []DDL
}

func (n *TransactionNode) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}

		switch ele := token.(type) {
		case xml.StartElement:
			name := ele.Name.Local
			var ddl DDL
			switch name {
			case "insert":
				ddl = &InsertNode{}
			case "update":
				ddl = &UpdateNode{}
			case "delete":
				ddl = &DeleteNode{}
			case "sqlInline":
				ddl = &SqlInlineNode{}
			case "sqlFile":
				ddl = &SqlFileNode{}
			default:
				return New("invalid DML element <%s> in transaction", name)
			}
			if err = decoder.DecodeElement(ddl, &ele); err != nil {
				return err
			}
			n.DMLs = append(n.DMLs, ddl)
		case xml.EndElement:
			if ele.Name.Local == start.Name.Local {
				return nil
			}
		}
	}
	return nil
}

func (n *TransactionNode) Execute(ctx context.Context, fly *Dbfly) error {
	fly.logger.Debug("transaction begin", "dmlCount", len(n.DMLs))
	tx, err := fly.driver.BeginTx(ctx)
	if err != nil {
		return Wrap(err, "failed to begin transaction")
	}
	// 设置事务上下文
	fly.tx = tx
	defer func() {
		fly.tx = nil
	}()
	for _, ddl := range n.DMLs {
		if err = ddl.Execute(ctx, fly); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				fly.logger.Error("transaction rollback failed", "error", rbErr)
				return New("operation failed: %w, rollback also failed: %w", err, rbErr)
			}
			fly.logger.Debug("transaction rolled back", "error", err)
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		fly.logger.Error("transaction commit failed", "error", err)
		return err
	}
	fly.logger.Debug("transaction committed")
	return nil
}

// writeColumnValue 写入列值
func writeColumnValue(builder *strings.Builder, col *DataColumnNode) {
	if col.OriginValue != "" {
		builder.WriteString(col.OriginValue)
	} else {
		builder.WriteString("'")
		builder.WriteString(strings.ReplaceAll(col.Value, "'", "''"))
		builder.WriteString("'")
	}
}

// IncludeNode 引用节点
type IncludeNode struct {
	File string `xml:"file,attr"`
}
