package examples

import (
	"embed"
	"fmt"
	"github.com/jianggujin/go-dbfly"
)

//go:embed sql
var sqlFiles embed.FS

func EmbedFSSourceExample() {
	source := dbfly.EmbedFSSource{
		Fs:    sqlFiles,
		Paths: []string{"sql"},
	}
	sources, err := source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err := source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}

func FSSourceExample() {
	source := dbfly.FSSource{
		Paths: []string{"sql"},
	}
	sources, err := source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err := source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}

func EmbedSourceExample() {
	source := dbfly.EmbedSource{
		Sources: map[string]*dbfly.EmbedSourceInfo{
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
	sources, err := source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err := source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}
