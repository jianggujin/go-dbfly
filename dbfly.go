package dbfly

import (
	"bytes"
	"context"
	"encoding/xml"
	"github.com/hashicorp/go-version"
	"io"
	"sort"
)

// 默认用于记录版本变化的表名
const changeTableName = "DBFLY_CHANGE_LOG"

type Dbfly struct {
	migratory       Migratory
	driver          Driver
	source          Source
	changeTableName string
}

func NewDbfly(migratory Migratory, driver Driver, source Source) *Dbfly {
	return &Dbfly{
		migratory: migratory,
		driver:    driver,
		source:    source,
	}
}

// SetChangeTableName 设置记录变更表名
func (f *Dbfly) SetChangeTableName(changeTableName string) {
	f.changeTableName = changeTableName
}

// Migrate 合并操作
func (f *Dbfly) Migrate() error {
	return f.MigrateContext(context.Background())
}

func (f *Dbfly) MigrateContext(ctx context.Context) error {
	if f.changeTableName == "" {
		f.changeTableName = changeTableName
	}
	// 1 查找版本
	sources, err := f.source.Scan()
	if err != nil {
		return err
	}
	// 1.1 不存在版本则直接返回
	if len(sources) == 0 {
		return nil
	}
	// 1.2 对版本信息进行排序
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Version.LessThan(sources[j].Version)
	})

	// 2 查询历史执行版本
	// 2.1 初始化变更记录表
	if err = f.migratory.InitChangeLogTable(ctx, f.driver, f.changeTableName); err != nil {
		return err
	}
	// 2.2 查询历史执行版本
	lastVersion, err := f.migratory.LastVersion(ctx, f.driver, f.changeTableName)
	if err != nil {
		return nil
	}
	// 3 合并版本
	for _, source := range sources {
		// 3.1 如果版本小于最后一次版本则忽略
		if lastVersion != nil && source.Version.LessThanOrEqual(lastVersion) {
			continue
		}
		// 3.2 执行升级
		nodes, err := f.parseContent(source)
		if err != nil {
			return err
		}
		if err = f.migrateNodes(ctx, nodes, source.Version); err != nil {
			return err
		}
	}
	return nil
}

func (f *Dbfly) migrateNodes(ctx context.Context, nodes []Node, version *version.Version) error {
	migratory := f.migratory
	driver := f.driver
	err := migratory.NewChangeLog(ctx, driver, f.changeTableName, version.Original())
	if err != nil {
		return err
	}
	for _, node := range nodes {
		switch n := node.(type) {
		case *CreateTableNode:
			err = migratory.CreateTable(ctx, driver, n)
		case *CreateIndexNode:
			err = migratory.CreateIndex(ctx, driver, n)
		case *CreatePrimaryKeyNode:
			err = migratory.CreatePrimaryKey(ctx, driver, n)
		case *DropTableNode:
			err = migratory.DropTable(ctx, driver, n)
		case *DropIndexNode:
			err = migratory.DropIndex(ctx, driver, n)
		case *AddColumnNode:
			err = migratory.AddColumn(ctx, driver, n)
		case *RenameColumnNode:
			err = migratory.RenameColumn(ctx, driver, n)
		case *AlterColumnNode:
			err = migratory.AlterColumn(ctx, driver, n)
		case *DropColumnNode:
			err = migratory.DropColumn(ctx, driver, n)
		case *DropPrimaryKeyNode:
			err = migratory.DropPrimaryKey(ctx, driver, n)
		case *RenameTableNode:
			err = migratory.RenameTable(ctx, driver, n)
		case *AlterTableRemarksNode:
			err = migratory.AlterTableRemarks(ctx, driver, n)
		case *ScriptNode:
			err = migratory.Script(ctx, driver, n)
		}
		if err != nil {
			return err
		}
	}
	return migratory.CompleteChangeLog(ctx, driver, f.changeTableName, version.Original())
}

// 解析源文件内容
func (f *Dbfly) parseContent(source *SourceInfo) ([]Node, error) {
	content, err := f.source.Read(source.Uid)
	if err != nil {
		return nil, err
	}
	// 如果是脚本，不需要解析
	if source.Script {
		return []Node{
			ScriptNode{
				Dialect: allDialect,
				Value:   string(content),
			},
		}, nil
	}

	decoder := xml.NewDecoder(bytes.NewReader(content))
	var nodes []Node
	// 遍历 XML 树
	for {
		// 读取下一个 token
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				return nodes, nil
			}
			return nil, err
		}

		// 判断 token 的类型
		switch token := token.(type) {
		case xml.StartElement:
			var node Node
			switch token.Name.Local {
			case "createTable":
				node = &CreateTableNode{}
			case "createIndex":
				node = &CreateIndexNode{}
			case "createPrimaryKey":
				node = &CreatePrimaryKeyNode{}
			case "dropTable":
				node = &DropTableNode{}
			case "dropIndex":
				node = &DropIndexNode{}
			case "addColumn":
				node = &AddColumnNode{}
			case "renameColumn":
				node = &RenameColumnNode{}
			case "alterColumn":
				node = &AlterColumnNode{}
			case "dropColumn":
				node = &DropColumnNode{}
			case "dropPrimaryKey":
				node = &DropPrimaryKeyNode{}
			case "renameTable":
				node = &RenameTableNode{}
			case "alterTableRemarks":
				node = &AlterTableRemarksNode{}
			case "script":
				node = &ScriptNode{}
			}
			if node != nil {
				if err = decoder.DecodeElement(node, &token); err != nil {
					return nil, err
				}
				nodes = append(nodes, node)
			}
		}
	}
}
