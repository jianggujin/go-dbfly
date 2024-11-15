package dbfly

import "testing"

func TestDbfly_Migrate(t *testing.T) {
	migratory := NewSqliteMigratory()
	driver := NewDryRunDriver()
	source := &EmbedSource{
		Sources: map[string]*EmbedSourceInfo{
			"v1.0.0": {
				Script: false,
				Content: []byte(`<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    <createTable tableName="t_config" remarks="配置信息表">
        <column columnName="config_key" dataType="VARCHAR" maxLength="100" primaryKey="true"
                remarks="配置键"/>
        <column columnName="config_value" dataType="TEXT" remarks="配置值"/>
        <column columnName="created_at" dataType="TIMESTAMP" nullable="false" remarks="创建时间"/>
        <column columnName="updated_at" dataType="TIMESTAMP" nullable="false" remarks="修改时间"/>
    </createTable>
</dbfly>`),
			},
		},
	}
	fly := NewDbfly(migratory, driver, source)
	if err := fly.Migrate(); err != nil {
		t.Fatal(err)
	}
}
