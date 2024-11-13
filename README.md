# 第一部分 概述
`go-dbfly`是一种数据库迁移工具，主要用于管理数据库架构的版本控制和变更。通过跟踪 SQL 脚本的执行顺序和状态来实现数据库的自动化迁移，使团队能够更轻松地管理数据库的演变，确保各个环境中的数据库保持一致。

## 1.1 作用

1. **数据库版本控制**：`go-dbfly`能够将数据库架构变更记录成一系列有序的脚本，每个脚本都带有唯一的版本号。通过这些脚本的版本控制，`go-dbfly`能确保在不同环境下执行相同的数据库更新操作，从而保持数据库结构的一致性。
2. **自动化迁移**：`go-dbfly`可以自动检测未执行的迁移脚本，并按顺序执行，确保数据库结构保持更新且符合当前的应用程序要求。
3. **多数据库兼容**：`go-dbfly`可以将迁移脚本组织得井井有条，通过`XML`的标准定义可生成对应数据库的执行脚本。

## 1.2 常见使用场景

1. **应用程序开发**：在开发过程中，开发人员经常需要变更数据库结构（如新增表、修改字段）。使用`go-dbfly`可以轻松跟踪这些变更并同步到其他开发或测试环境。
2. **持续集成/持续交付（CI/CD）**：在 `CI/CD` 环境中，`go-dbfly`可以自动执行数据库迁移，确保每次部署应用时数据库结构也是最新的，减少手动管理数据库变更的风险。
3. **多环境数据库同步**：在企业级应用中，经常会有开发、测试、预生产、生产等多个环境，`go-dbfly`可以保证每个环境中的数据库结构一致。
4. **数据库审计和合规性管理**：在对数据库变更记录有严格要求的场景下，`go-dbfly`也可以充当一个数据库审计工具，帮助团队记录和追踪每一次变更，以满足合规性要求。

## 1.3 支持数据库

- MySQL
- Sqlite
- PostgresSql

# 第二部分 快速开始

## 2.1 安装

```bash
$ go get github.com/jianggujin/go-dbfly
```

**提示:** `go-dbfly`使用 [Go Modules](https://go.dev/wiki/Modules) 管理依赖。

## 2.2 编写定义文件

`go-dbfly`的定义文件支持两种方式：

- **XML**: 通过内置约束定义，升级时将定义转换为对应数据库的执行语句，兼容性高，适合大部分场景
- **SQL**: 编写原始的SQL语句，升级过程中会将其拆分为最小可执行单元进行执行，不支持多数据库兼容，必须使用标准SQL语句，避免出现数据库切换导致实行失败，不建议使用

### 2.2.1 文件名命名规则

版本号-描述信息.文件后缀

- 版本号: 版本号需要遵循[SemVer](https://semver.org/)规范，解析器解析完成后会按照从小到大顺序执行
- 描述信息: 用于说明版本的描述信息，仅作记录，无实际作用，不影响脚本执行
- 文件后缀: 支持`xml`、`sql`两种形式

### 2.2.2 定义文件

 **XML文件**

```xml
<?xml version="1.0"?>
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
</dbfly>
```

**SQL文件**

```sql
create table t_config (
    config_key VARCHAR(100) primary key,
    config_value TEXT,
    created_at TIMESTAMP noy null,
    updated_at TIMESTAMP not null
);
```

## 2.3 执行升级

```go
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
```

执行升级的核心是初始化合并器以及脚本源，最后通过`DbFly`的`Migrate()`方法完成合并，上述代码仅为演示。

# 第三部分 详细说明

## 3.1 数据源

`go-dbfly`在执行`SQL`合并时，需要获取当前可用的脚本信息，数据源即为`go-dbfly`提供了原始脚本，合并器通过解析数据源中的信息以生成相应的`SQL`语句。

在`go-dbfly`中目前内置了3种数据源实现，分别为：`嵌入文件数据源`、`文件数据源`、`嵌入数据源`，在实际使用中可以按照需要进行选择。

- **嵌入文件数据源(EmbedFSSource)**: 使用`go`自带的`Embed`资源实现，可以将定义文件与程序打包在一起

- **文件数据源(FSSource)**: 使用系统本身的文件系统，定义文件与程序分开

- **嵌入数据源(EmbedSource)**: 适用于较为简单的方式，将定义内容直接作为程序代码一部分嵌入

### 3.1.1 嵌入文件数据源(EmbedFSSource)

嵌入文件数据源用于将合并脚本与Go程序打包在一起的成情况，可以使用Go原生提供的的`Embed`资源实现，使用该种方式，通常情况下，我们应该选用该种方式。

使用示例如下：

```go
import (
	"embed"
	"fmt"
	"github.com/jianggujin/go-dbfly"
)

//go:embed sql
var sqlFiles embed.FS

func EmbedFSSourceExample() {
	source:= dbfly.EmbedFSSource{
		Fs:    sqlFiles,
		Paths: []string{"sql"},
	}
	sources, err:= source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err:= source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}
```

### 3.1.2 文件数据源(FSSource)

文件数据源用于将合并脚本存放于磁盘的指定位置，与主程序分离，利用系统本身的文件系统进行组织。

使用示例如下：

```go
import (
	"fmt"
	"github.com/jianggujin/go-dbfly"
)

func FSSourceExample() {
	source:= dbfly.FSSource{
		Paths: []string{"sql"},
	}
	sources, err:= source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err:= source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}
```

### 3.1.3 嵌入数据源(EmbedSource)

嵌入数据源适用于比较简单的场景，比如临时测试，合并脚本较为简单，无需使用独立的文件进行组织，该种方式合并脚本会作为程序代码的一部分嵌入。

使用示例如下：

```go
import (
	"fmt"
	"github.com/jianggujin/go-dbfly"
)

func EmbedSourceExample() {
	source:= dbfly.EmbedSource{
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
	sources, err:= source.Scan()
	if err != nil {
		panic(err)
	}
	for _, src := range sources {
		content, err:= source.Read(src.Uid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("uid: %s, version: %s, script: %v, content: %s\n", src.Uid, src.Version.String(), src.Script, string(content))
	}
}
```

### 3.1.4 自定义数据源

如果内置的数据源无法满足实际场景，可根据情况自定义数据源，自定义数据源需要实现`Source`接口，该接口有两个方法，分别为：

- `Scan() ([]*SourceInfo, error)`: 该方法用于扫描相应数据源中所有可用的脚本信息，由后续主程序执行
- `Read(string) ([]byte, error)`: 当主程序判断该版本脚本需要执行时，通过调用此方法获取脚本的详细内容

```go
// SQL源信息
type Source interface {
	// 扫描源中包含的需要合并的文件信息
	Scan() ([]*SourceInfo, error)
	// 读取源中指定uid的文件内容
	Read(string) ([]byte, error)
}
```

## 3.2 SQL执行驱动

`go-dbfly`在执行`SQL`合并时，需要执行转换后的`DDL`语句，`go-dbfly`中目前内置了2种SQL执行驱动实现，分别为：`标准SQL执行驱动`、`试运行执行驱动`，在实际使用中可以按照需要进行选择。

- **标准SQL执行驱动(SqlDriver)**: 使用`go`标准的`db`执行SQL语句，也是实际生产中主要的执行方式
- **试运行执行驱动(DryRunDriver)**: 仅打印转换后的SQL语句而不进行实际执行，适用于查看升级脚本转换结果的场景

### 3.2.1 标准SQL执行驱动

使用`go`标准的`db`执行SQL语句，也是实际生产中主要的执行方式。

使用示例如下：

```go
import (
	"context"
	"database/sql"
	"github.com/jianggujin/go-dbfly"
)

func SqlDriverExample()  {
	var db *sql.DB
	sql := ""
	driver:= dbfly.NewSqlDriver(db)
	err := driver.Execute(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}
```

### 3.2.2 试运行执行驱动

仅打印转换后的SQL语句而不进行实际执行，适用于查看升级脚本转换结果的场景。

使用示例如下：

```go
import (
	"context"
	"github.com/jianggujin/go-dbfly"
)

func DryRunDriverExample()  {
	sql := ""
	driver:= dbfly.NewDryRunDriver()
	err := driver.Execute(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}
```

### 3.2.3 自定义执行驱动

如果内置的执行驱动无法满足实际场景，可根据情况自定义执行驱动，自定义执行驱动需要实现`Driver`接口，该接口有两个方法，分别为：

- `Execute(context.Context, string, ...interface{}) error`: 该方法用于执行DDL语句或修改类操作
- `Query(context.Context, string, ...interface{}) (Rows, error)`: 该方法用于执行查询

```go
// 不同框架对数据库操作的驱动接口
type Driver interface {
	// 执行SQL
	Execute(context.Context, string, ...interface{}) error
	// 查询
	Query(context.Context, string, ...interface{}) (Rows, error)
}
```

## 3.3 合并器

合并器用于将标准的SQL定义脚本转换为实际数据库所需的SQL语句。

### 3.3.1 内置合并器

`go-dbfly`中默认提供了如下几种合并器

- **MysqlMigratory**: 适应于MySql数据库的合并器，可使用`dbfly.NewMysqlMigratory()`创建
- **SqliteMigratory**: 适应于Sqlite数据库的合并器，可使用`dbfly.NewSqliteMigratory()`创建
- **PostgresMigratory**: 适应于Postgres数据库的合并器，可使用`dbfly.NewPostgresMigratory()`创建

### 3.3.2 自定义合并器

```go
// SQL版本合并接口
type Migratory interface {
	// 合并器名称
	Name() string
	// 初始化记录变更记录表
	InitChangeLogTable(context.Context, Driver, string) error
	// 最后一次版本信息
	LastVersion(context.Context, Driver, string) (*version.Version, error)
	// 创建一条新的表更记录
	NewChangeLog(context.Context, Driver, string, string) error
	// 完成一条表更记录
	CompleteChangeLog(context.Context, Driver, string, string) error
	// 创建表
	CreateTable(context.Context, Driver, *CreateTableNode) error
	// 创建索引
	CreateIndex(context.Context, Driver, *CreateIndexNode) error
	// 创建主键
	CreatePrimaryKey(context.Context, Driver, *CreatePrimaryKeyNode) error
	// 删除表
	DropTable(context.Context, Driver, *DropTableNode) error
	// 删除索引
	DropIndex(context.Context, Driver, *DropIndexNode) error
	// 添加列
	AddColumn(context.Context, Driver, *AddColumnNode) error
	// 修改列
	AlterColumn(context.Context, Driver, *AlterColumnNode) error
	// 删除列
	DropColumn(context.Context, Driver, *DropColumnNode) error
	// 删除主键
	DropPrimaryKey(context.Context, Driver, *DropPrimaryKeyNode) error
	// 重命名表
	RenameTable(context.Context, Driver, *RenameTableNode) error
	// 修改表说明
	AlterTableRemarks(context.Context, Driver, *AlterTableRemarksNode) error
	// 执行自定义SQL脚本
	Script(context.Context, Driver, *ScriptNode) error
}
```

