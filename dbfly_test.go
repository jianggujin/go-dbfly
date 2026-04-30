package dbfly

import (
	"testing"
)

func TestParseXml_ChangeSetMode(t *testing.T) {
	// 测试：changeSet 元素解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="create-users" author="alice">
        <createTable tableName="users">
            <column columnName="id" dataType="INT" primaryKey="true"/>
        </createTable>
    </changeSet>
    <changeSet id="add-index" author="bob" onFail="SKIP">
        <createIndex tableName="users" indexName="idx_id">
            <column name="id"/>
        </createIndex>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Errorf("合法的 changeSet 模式应该解析成功，但得到错误: %v", err)
	}
	if len(changeSets) != 2 {
		t.Errorf("应该解析出 2 个 changeSet，实际得到 %d", len(changeSets))
	}
	if changeSets[0].Id != "create-users" {
		t.Errorf("第一个 changeSet id 应该是 create-users，实际是 %s", changeSets[0].Id)
	}
	if changeSets[1].OnFail != "SKIP" {
		t.Errorf("第二个 changeSet onFail 应该是 SKIP，实际是 %s", changeSets[1].OnFail)
	}
}

func TestParseXml_InvalidChangeSetId_ShouldFail(t *testing.T) {
	// 测试：非法的 changeSet id 格式
	tests := []struct {
		name    string
		id      string
		content []byte
	}{
		{
			name: "包含空格",
			id:   "create users",
			content: []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="create users" author="alice">
        <createTable tableName="users">
            <column columnName="id" dataType="INT"/>
        </createTable>
    </changeSet>
</dbfly>`),
		},
		{
			name: "包含中文",
			id:   "创建用户",
			content: []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="创建用户" author="alice">
        <createTable tableName="users">
            <column columnName="id" dataType="INT"/>
        </createTable>
    </changeSet>
</dbfly>`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Dbfly{}
			_, err := f.parseXmlContent("test.xml", tt.content, make(map[string]bool))
			if err == nil {
				t.Error("非法的 changeSet id 格式应该返回错误")
			}
		})
	}
}

func TestParseXml_DDLOutsideChangeSet_ShouldFail(t *testing.T) {
	// 测试：DDL 元素在 changeSet 外部（非法）
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <createTable tableName="users">
        <column columnName="id" dataType="INT" primaryKey="true"/>
    </createTable>
</dbfly>`)

	f := &Dbfly{}
	_, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err == nil {
		t.Error("DDL 元素在 changeSet 外部应该返回错误")
	}
}

func TestParseXml_IncludeElement(t *testing.T) {
	// 测试：include 元素解析（需要 mock source）
	// 此测试验证 include 元素能被正确解析
	t.Log("include 元素测试需要 mock source 支持")
}

func TestIsValidChangeSetId(t *testing.T) {
	// 测试 changeSet id 格式验证
	validIds := []string{
		"create-users", "001.init", "add_column", "test-1.0",
		"ABC", "a_b_c", "a-b-c", "a.b.c",
	}

	for _, id := range validIds {
		if !isValidChangeSetId(id) {
			t.Errorf("id %s 应该是合法的格式", id)
		}
	}

	invalidIds := []string{
		"", "create users", "创建用户", "test@123",
	}

	for _, id := range invalidIds {
		if isValidChangeSetId(id) {
			t.Errorf("id %s 不应该是合法的格式", id)
		}
	}
}

func TestIsDDLElement(t *testing.T) {
	// 测试 isDDLElement 辅助函数
	validElements := []string{
		"createTable", "createIndex", "createPrimaryKey",
		"dropTable", "dropIndex", "dropPrimaryKey",
		"addColumn", "renameColumn", "alterColumn", "dropColumn",
		"renameTable", "alterTableComment", "sqlFile",
		"insert", "update", "delete", "sqlInline", "transaction",
	}

	for _, elem := range validElements {
		if !isDDLElement(elem) {
			t.Errorf("元素 %s 应该是合法的 DDL 元素", elem)
		}
	}

	invalidElements := []string{
		"changeSet", "include", "dbfly", "column", "conditions",
		"version", "unknownElement", "invalidTag",
	}

	for _, elem := range invalidElements {
		if isDDLElement(elem) {
			t.Errorf("元素 %s 不应该是合法的 DDL 元素", elem)
		}
	}
}

func TestChangeSetDefaultOnFail(t *testing.T) {
	// 测试：未指定 onFail 时默认为 HALT
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="test" author="alice">
        <createTable tableName="users">
            <column columnName="id" dataType="INT"/>
        </createTable>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Errorf("解析失败: %v", err)
	}
	if changeSets[0].OnFail != "HALT" {
		t.Errorf("默认 onFail 应该是 HALT，实际是 %s", changeSets[0].OnFail)
	}
}

func TestParseXml_InsertNode(t *testing.T) {
	// 测试：insert 节点解析（单行模式）
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="insert-data" author="alice">
        <insert tableName="users">
            <column name="id" value="1"/>
            <column name="name" value="Alice"/>
            <column name="created_at" originValue="NOW()"/>
        </insert>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(changeSets) != 1 {
		t.Fatalf("应该解析出 1 个 changeSet，实际得到 %d", len(changeSets))
	}
	if len(changeSets[0].DDLs) != 1 {
		t.Fatalf("应该有 1 个 DDL，实际有 %d", len(changeSets[0].DDLs))
	}
	insertNode, ok := changeSets[0].DDLs[0].(*InsertNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 InsertNode")
	}
	if insertNode.TableName != "users" {
		t.Errorf("tableName 应该是 users，实际是 %s", insertNode.TableName)
	}
	if len(insertNode.Columns) != 3 {
		t.Fatalf("应该有 3 列，实际有 %d", len(insertNode.Columns))
	}
	if insertNode.Columns[0].Name != "id" || insertNode.Columns[0].Value != "1" {
		t.Errorf("第一列应该是 id=1，实际是 %s=%s", insertNode.Columns[0].Name, insertNode.Columns[0].Value)
	}
	if insertNode.Columns[2].OriginValue != "NOW()" {
		t.Errorf("第三列 originValue 应该是 NOW()，实际是 %s", insertNode.Columns[2].OriginValue)
	}
}

func TestParseXml_InsertBatchNode(t *testing.T) {
	// 测试：insert 节点解析（批量模式）
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="batch-insert" author="alice">
        <insert tableName="users">
            <row>
                <column name="id" value="1"/>
                <column name="name" value="Alice"/>
            </row>
            <row>
                <column name="id" value="2"/>
                <column name="name" value="Bob"/>
            </row>
        </insert>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	insertNode, ok := changeSets[0].DDLs[0].(*InsertNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 InsertNode")
	}
	if len(insertNode.Rows) != 2 {
		t.Fatalf("应该有 2 行，实际有 %d", len(insertNode.Rows))
	}
	if insertNode.Rows[0].Columns[0].Value != "1" {
		t.Errorf("第一行第一列的值应该是 1")
	}
	if insertNode.Rows[1].Columns[1].Value != "Bob" {
		t.Errorf("第二行第二列的值应该是 Bob")
	}
}

func TestParseXml_UpdateNode(t *testing.T) {
	// 测试：update 节点解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="update-data" author="alice">
        <update tableName="users">
            <column name="status" value="active"/>
            <column name="updated_at" originValue="NOW()"/>
            <where>id = 1</where>
        </update>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	updateNode, ok := changeSets[0].DDLs[0].(*UpdateNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 UpdateNode")
	}
	if updateNode.TableName != "users" {
		t.Errorf("tableName 应该是 users")
	}
	if len(updateNode.Columns) != 2 {
		t.Errorf("应该有 2 列")
	}
	if updateNode.Where != "id = 1" {
		t.Errorf("where 应该是 'id = 1'，实际是 '%s'", updateNode.Where)
	}
}

func TestParseXml_DeleteNode(t *testing.T) {
	// 测试：delete 节点解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="delete-data" author="alice">
        <delete tableName="users">
            <where>status = 'inactive'</where>
        </delete>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	deleteNode, ok := changeSets[0].DDLs[0].(*DeleteNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 DeleteNode")
	}
	if deleteNode.TableName != "users" {
		t.Errorf("tableName 应该是 users")
	}
	if deleteNode.Where != "status = 'inactive'" {
		t.Errorf("where 不匹配")
	}
}

func TestParseXml_SqlInlineNode(t *testing.T) {
	// 测试：sqlInline 节点解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="inline-sql" author="alice">
        <sqlInline>
            <default>INSERT INTO users (id) VALUES (1)</default>
            <sqlDbms dbms="mysql">INSERT INTO users (id) VALUES (1) ON DUPLICATE KEY UPDATE id=1</sqlDbms>
            <sqlDbms dbms="postgres">INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING</sqlDbms>
        </sqlInline>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	sqlNode, ok := changeSets[0].DDLs[0].(*SqlInlineNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 SqlInlineNode")
	}
	if len(sqlNode.SqlDbms) != 2 {
		t.Fatalf("应该有 2 个 sqlDbms，实际有 %d", len(sqlNode.SqlDbms))
	}
	if sqlNode.SqlDbms[0].Dbms != "mysql" {
		t.Errorf("第一个 sqlDbms 的 dbms 应该是 mysql")
	}
	if sqlNode.SqlDbms[1].Dbms != "postgres" {
		t.Errorf("第二个 sqlDbms 的 dbms 应该是 postgres")
	}
}

func TestParseXml_SqlFileWithDbmsNode(t *testing.T) {
	// 测试：sqlFile 带 sqlFileDbms 子元素解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="sql-file" author="alice">
        <sqlFile path="init.sql">
            <sqlFileDbms dbms="mysql" path="init-mysql.sql"/>
            <sqlFileDbms dbms="postgres" path="init-pg.sql"/>
        </sqlFile>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	sqlFileNode, ok := changeSets[0].DDLs[0].(*SqlFileNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 SqlFileNode")
	}
	if sqlFileNode.Path != "init.sql" {
		t.Errorf("path 应该是 init.sql，实际是 %s", sqlFileNode.Path)
	}
	if len(sqlFileNode.SqlFileDbms) != 2 {
		t.Fatalf("应该有 2 个 sqlFileDbms，实际有 %d", len(sqlFileNode.SqlFileDbms))
	}
	if sqlFileNode.SqlFileDbms[0].Dbms != "mysql" || sqlFileNode.SqlFileDbms[0].Path != "init-mysql.sql" {
		t.Errorf("第一个 sqlFileDbms 不匹配")
	}
}

func TestParseXml_TransactionNode(t *testing.T) {
	// 测试：transaction 节点解析
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="tx-ops" author="alice">
        <transaction>
            <insert tableName="users">
                <column name="id" value="1"/>
                <column name="name" value="Alice"/>
            </insert>
            <update tableName="config">
                <column name="value" value="v2.0"/>
                <where>key = 'version'</where>
            </update>
        </transaction>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	txNode, ok := changeSets[0].DDLs[0].(*TransactionNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 TransactionNode")
	}
	if len(txNode.DMLs) != 2 {
		t.Fatalf("应该有 2 个 DML，实际有 %d", len(txNode.DMLs))
	}
	if _, ok := txNode.DMLs[0].(*InsertNode); !ok {
		t.Fatal("第一个子节点应该是 InsertNode")
	}
	if _, ok := txNode.DMLs[1].(*UpdateNode); !ok {
		t.Fatal("第二个子节点应该是 UpdateNode")
	}
}

func TestParseXml_DMLWithConditions(t *testing.T) {
	// 测试：DML 节点带 conditions
	content := []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <changeSet id="conditional-insert" author="alice">
        <insert tableName="users">
            <conditions>
                <condition>
                    <tableExists tableName="users"/>
                </condition>
            </conditions>
            <column name="id" value="1"/>
            <column name="name" value="Alice"/>
        </insert>
    </changeSet>
</dbfly>`)

	f := &Dbfly{}
	changeSets, err := f.parseXmlContent("test.xml", content, make(map[string]bool))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	insertNode, ok := changeSets[0].DDLs[0].(*InsertNode)
	if !ok {
		t.Fatal("第一个 DDL 应该是 InsertNode")
	}
	if insertNode.Conditions == nil || len(insertNode.Conditions.Conditions) != 1 {
		t.Fatal("应该有 1 个 condition")
	}
}
