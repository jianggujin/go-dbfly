# go-dbfly

`go-dbfly` 是一个 Go 语言数据库迁移工具，采用 changeSet 驱动的 changelog 模型管理数据库架构变更。通过 XML 定义的 DSL 描述 DDL 和 DML 操作，自动生成适配多种数据库的 SQL 语句，确保各环境数据库结构一致。

## 目录

- [概述](#概述)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [XML定义指南](#xml定义指南)
- [组件配置](#组件配置)
- [高级特性](#高级特性)
- [附录](#附录)

---

# 概述

## 核心特性

- **changeSet 驱动**：每个变更集拥有唯一标识，已执行的不会重复执行
- **多数据库兼容**：统一 XML 定义，自动生成数据库特定 SQL
- **DDL + DML 支持**：表结构操作与数据操作一体化管理
- **条件执行**：支持表/列/索引存在性检查、数据库类型匹配等前置条件
- **事务控制**：DML 操作可包装在事务中原子执行
- **方言适配**：列类型、表属性、SQL 语句均可按数据库定制

## 使用场景

| 场景 | 说明 |
|------|------|
| 应用开发 | 跟踪数据库结构变更，同步到开发/测试环境 |
| CI/CD | 自动执行迁移，确保部署时数据库结构最新 |
| 多环境同步 | 保证开发、测试、预生产、生产环境数据库一致 |
| 审计合规 | 记录每次变更的作者、时间、来源文件 |

## 支持数据库

| 数据库 | DBMS 名称 | 引号 | 特殊行为 |
|--------|-----------|------|---------|
| MySQL | MySQL | `` ` `` | 内联 COMMENT、RENAME TABLE、ENGINE/CHARSET 属性 |
| SQLite | SQLite | `` ` `` | ALTER 操作需重建表，不支持 COMMENT |
| PostgreSQL | PostgreSQL | `"` | BOOLEAN→SMALLINT、BLOB→BYTEA 映射 |
| Oracle | Oracle | `"` | VARCHAR→VARCHAR2、BOOLEAN→NUMBER(1) 映射 |
| DaMeng | DM DBMS | `"` | 类 Oracle，使用相同系统视图 |
| Vastbase | VastBase | `"` | 包装 PostgreSQL |

## 支持数据类型

XML 定义中使用标准类型，迁移器自动转换：

| 标准类型 | 说明 |
|---------|------|
| VARCHAR | 可变长度字符串 |
| CHAR | 定长字符串 |
| TEXT | 文本类型 |
| CLOB | 大字符对象 |
| BOOLEAN | 布尔类型 |
| TINYINT | 微整数 |
| SMALLINT | 小整数 |
| INT | 整数 |
| BIGINT | 大整数 |
| DECIMAL | 精确数值 |
| DATE | 日期 |
| TIME | 时间 |
| TIMESTAMP | 时间戳 |
| BLOB | 二进制大对象 |

---

# 快速开始

## 安装

```bash
go get github.com/jianggujin/go-dbfly
```

## 基础示例

### 入口文件

默认入口文件为 `dbfly.xml`，通过 `<include>` 引用其他文件：

```xml
<?xml version="1.0"?>
<dbfly xmlns="https://www.jianggujin.com/c/xml/dbfly"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://www.jianggujin.com/c/xml/dbfly
        https://www.jianggujin.com/c/xml/dbfly.xsd">
    
    <!-- 引入模块 -->
    <include file="core/dbfly.xml"/>
    
    <!-- 变更集定义 -->
    <changeSet id="create-config" author="system">
        <createTable tableName="t_config" comment="配置表">
            <column columnName="key" dataType="VARCHAR" maxLength="100" primaryKey="true"/>
            <column columnName="value" dataType="TEXT"/>
            <column columnName="created_at" dataType="TIMESTAMP" nullable="false"/>
        </createTable>
    </changeSet>
</dbfly>
```

### changeSet 属性

| 属性 | 必填 | 说明 |
|------|------|------|
| id | 是 | 唯一标识，正则 `^[a-zA-Z0-9_\-\.]+$` |
| author | 否 | 作者 |
| onFail | 否 | 失败策略：`HALT`（默认，停止）、`SKIP`（跳过继续） |

## 执行迁移

```go
import "github.com/jianggujin/go-dbfly"

// 初始化组件
migratory := dbfly.NewMysqlMigratory()
driver := dbfly.NewSqlDriver(db)  // db 为 *sql.DB
source := dbfly.NewLocalFSSource("migrations")

// 执行迁移
fly := dbfly.NewDbfly(migratory, driver, source)
if err := fly.Migrate(); err != nil {
    panic(err)
}
```

---

# 核心概念

## 架构设计

`go-dbfly` 采用插件化架构，三个核心接口构成扩展点：

```
┌─────────────────────────────────────────────────────────┐
│                      Dbfly                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────────────────────┐  │
│  │ Source  │  │ Driver  │  │      Migratory          │  │
│  │ (读取)  │  │ (执行)  │  │ (DDL/DML → SQL转换)     │  │
│  └─────────┘  └─────────┘  └─────────────────────────┘  │
│                                                         │
│  ┌─────────┐  ┌─────────┐                              │
│  │ Locker  │  │Recorder │  (执行状态管理)              │
│  └─────────┘  └─────────┘                              │
└─────────────────────────────────────────────────────────┘
```

| 接口 | 作用 | 内置实现 |
|------|------|---------|
| Source | 读取迁移脚本 | FSSource（embed/本地文件系统） |
| Driver | 执行 SQL | SqlDriver（包装 *sql.DB） |
| Migratory | 转换 DDL/DML | DefaultMigratory + 数据库覆盖 |
| Locker | 防止并发迁移 | DbLocker（乐观版本锁） |
| Recorder | 记录执行历史 | DbRecorder（changeSet 状态） |

## 变更集模型

### 执行流程

`Dbfly.MigrateContext()` 执行步骤：

1. **解析 changelog**：从入口文件递归解析，支持 `<include>` 循环检测
2. **检测重复 ID**：changeSet ID 全局唯一，重复则报错
3. **获取锁**：通过 Locker 获取排他锁，防止并发执行
4. **初始化记录表**：创建 `DBFLY_CHANGE_LOG`（如不存在）
5. **获取已执行列表**：查询已成功执行的 changeSet ID
6. **执行变更集**：
   - 跳过已执行的 changeSet
   - 检查 conditions，不满足则跳过
   - 执行所有 DDL/DML 节点
   - 根据 onFail 处理错误
7. **释放锁**

### 执行顺序

- changeSet 按 changelog 定义顺序执行
- `<include>` 内容插入到当前位置
- 已执行的 changeSet（ID 匹配）不重复执行

## 锁与记录机制

### 锁机制（DbLocker）

基于 `DBFLY_CHANGE_LOCK` 表的乐观版本锁：

| 字段 | 类型 | 说明 |
|------|------|------|
| ID | INT | 主键（固定为 1） |
| IS_LOCKED | TINYINT | 锁定状态 |
| LOCKED_BY | VARCHAR(255) | 锁定者主机名 |
| LOCK_TIME | TIMESTAMP | 锁定时间 |
| VERSION | INT | 版本号（乐观锁） |

特性：
- 重试间隔：100ms（可配置）
- 超时时间：30s（可配置）
- TOCTOU 防护：版本号验证

### 记录器（DbRecorder）

基于 `DBFLY_CHANGE_LOG` 表记录执行历史：

| 字段 | 类型 | 说明 |
|------|------|------|
| CHANGESET_ID | VARCHAR(255) | 变更集 ID（主键） |
| AUTHOR | VARCHAR(255) | 作者 |
| FILENAME | VARCHAR(255) | 来源文件 |
| ORDER_EXECUTED | INT | 执行顺序 |
| IS_SUCCESS | TINYINT | 成功状态 |
| CREATED_AT | TIMESTAMP | 创建时间 |
| UPDATED_AT | TIMESTAMP | 更新时间 |

---

# XML定义指南

## 文件组织

### 入口文件与引用

```xml
<dbfly>
    <!-- 模块引用 -->
    <include file="core/dbfly.xml"/>
    
    <!-- 当前文件定义 -->
    <changeSet id="global-setup">...</changeSet>
    
    <!-- 更多引用 -->
    <include file="features/order/dbfly.xml"/>
</dbfly>
```

执行顺序：core → global-setup → order

## DDL 操作

### 表操作

| 元素 | 说明 | 属性 |
|------|------|------|
| createTable | 创建表 | tableName, comment |
| dropTable | 删除表 | tableName |
| renameTable | 重命名表 | tableName, newTableName |
| alterTableComment | 修改表注释 | tableName, comment |

```xml
<createTable tableName="users" comment="用户表">
    <column columnName="id" dataType="BIGINT" primaryKey="true"/>
    <column columnName="name" dataType="VARCHAR" maxLength="100" nullable="false"/>
</createTable>
```

### 列操作

| 元素 | 说明 | 属性 |
|------|------|------|
| addColumn | 添加列 | tableName |
| alterColumn | 修改列 | tableName, columnName |
| renameColumn | 重命名列 | tableName, columnName, newColumnName |
| dropColumn | 删除列 | tableName, columnName |

```xml
<addColumn tableName="users">
    <column columnName="email" dataType="VARCHAR" maxLength="200"/>
</addColumn>
```

### 索引与主键

| 元素 | 说明 | 属性 |
|------|------|------|
| createIndex | 创建索引 | tableName, indexName, unique |
| createPrimaryKey | 创建主键 | tableName, keyName |
| dropIndex | 删除索引 | tableName, indexName |
| dropPrimaryKey | 删除主键 | tableName |

```xml
<createIndex tableName="users" indexName="idx_email" unique="true">
    <column name="email"/>
</createIndex>
```

### 列定义属性

| 属性 | 说明 |
|------|------|
| columnName | 列名 |
| dataType | 数据类型（标准类型） |
| maxLength | 长度（VARCHAR/CHAR） |
| numericScale | 小数位数（DECIMAL） |
| nullable | 是否允许空值 |
| primaryKey | 是否主键 |
| unique | 是否唯一 |
| defaultValue | 默认值（自动引用） |
| defaultOriginValue | 默认值（原始 SQL） |
| comment | 列注释 |

## DML 操作

### insert 插入数据

**单行插入**：
```xml
<insert tableName="users">
    <column name="id" value="1"/>
    <column name="name" value="Alice"/>
    <column name="created_at" originValue="NOW()"/>
</insert>
```

**批量插入**：
```xml
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
```

列值处理：
- `value="值"`：字符串值，自动单引号包裹
- `originValue="NOW()"`：原始 SQL 表达式，不包裹

### update 更新数据

```xml
<update tableName="users">
    <column name="status" value="active"/>
    <column name="updated_at" originValue="NOW()"/>
    <where>id = 1</where>
</update>
```

### delete 删除数据

```xml
<delete tableName="users">
    <where>status = 'inactive'</where>
</delete>
```

### transaction 事务控制

将多个 DML 操作原子执行：

```xml
<transaction>
    <insert tableName="orders">
        <column name="id" value="1"/>
    </insert>
    <insert tableName="order_items">
        <column name="order_id" value="1"/>
    </insert>
</transaction>
```

**注意**：transaction 仅允许包含 DML（insert/update/delete/sqlInline/sqlFile）。

⚠️ **DDL 风险警告**

`sqlInline` 和 `sqlFile` 可包含任意 SQL 语句，包括 DDL（如 CREATE TABLE、DROP TABLE）。但不同数据库对事务内 DDL 的行为不一致：

| 数据库 | 事务内 DDL 行为 | 风险 |
|--------|----------------|------|
| MySQL | DDL 会隐式提交事务 | 破坏原子性 |
| PostgreSQL | DDL 支持事务回滚 | ✓ 安全 |
| Oracle | DDL 会隐式提交事务 | 破坏原子性 |
| SQLite | DDL 支持事务回滚 | ✓ 安全 |
| DaMeng | DDL 会隐式提交事务 | 破坏原子性 |
| VastBase | DDL 支持事务回滚 | ✓ 安全 |

**建议**：transaction 内的 `sqlInline`/`sqlFile` 应仅包含 DML 语句（INSERT/UPDATE/DELETE），避免 DDL 语句（CREATE/DROP/ALTER）。如需执行 DDL，请将其放在 transaction 外部。

## SQL 方言选择

### sqlInline 内联 SQL

```xml
<sqlInline>
    <default>INSERT INTO users (id) VALUES (1)</default>
    <sqlDbms dbms="MySQL">INSERT INTO users (id) VALUES (1) ON DUPLICATE KEY UPDATE id=id</sqlDbms>
    <sqlDbms dbms="PostgreSQL">INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING</sqlDbms>
</sqlInline>
```

### sqlFile SQL 文件

```xml
<sqlFile path="init.sql">
    <sqlFileDbms dbms="MySQL" path="init-mysql.sql"/>
    <sqlFileDbms dbms="PostgreSQL" path="init-pg.sql"/>
</sqlFile>
```

系统根据当前数据库类型自动选择对应版本。

## 执行条件

所有节点支持 `<conditions>` 块，组间为 OR，组内为 AND：

### 支持的条件

| 条件 | 说明 | 属性 |
|------|------|------|
| tableExists | 表是否存在 | tableName, not |
| columnExists | 列是否存在 | tableName, columnName, not |
| primaryKeyExists | 主键是否存在 | tableName, not |
| indexExists | 索引是否存在 | tableName, indexName, not |
| rowCount | 行数检查 | tableName, expectedRows, not |
| sqlCheck | SQL 查询验证 | expectedResult, not |
| dbms | 数据库类型匹配 | name, not |

### 条件示例

```xml
<createTable tableName="audit_logs">
    <conditions>
        <!-- 组1：表不存在时创建 -->
        <condition>
            <tableExists tableName="audit_logs" not="true"/>
        </condition>
        <!-- 组2：或 MySQL 环境下创建 -->
        <condition>
            <dbms name="MySQL"/>
        </condition>
    </conditions>
    <column columnName="id" dataType="BIGINT" primaryKey="true"/>
</createTable>
```

## 数据库方言适配

### 列方言（columnDbms）

不同数据库使用不同列类型：

```xml
<createTable tableName="t_data">
    <column columnName="content" dataType="TEXT">
        <columnDbms dbms="MySQL" dataType="LONGTEXT"/>
        <columnDbms dbms="Oracle" dataType="CLOB"/>
        <columnDbms dbms="PostgreSQL" dataType="TEXT"/>
    </column>
</createTable>
```

### 表属性（dbmsAttributes）

数据库特定表属性：

```xml
<createTable tableName="t_log">
    <column columnName="id" dataType="BIGINT" primaryKey="true"/>
    <dbmsAttributes>
        <attribute dbms="MySQL" name="ENGINE" value="InnoDB"/>
        <attribute dbms="MySQL" name="CHARSET" value="utf8mb4"/>
    </dbmsAttributes>
</createTable>
```

---

# 组件配置

## 数据源（Source）

### 嵌入文件数据源

使用 `embed.FS`，脚本与程序打包：

```go
//go:embed sql
var sqlFiles embed.FS

// 单文件系统
source := dbfly.NewEmbedFSSource(sqlFiles)

// 多文件系统（依次尝试）
source := dbfly.NewEmbedFSSource(coreFiles, featureFiles)
```

### 本地文件数据源

使用系统文件系统：

```go
// 单目录
source := dbfly.NewLocalFSSource("migrations")

// 多目录（依次尝试）
source := dbfly.NewLocalFSSource("core", "features", "sql")
```

### 自定义数据源

实现 `Source` 接口：

```go
type Source interface {
    Read(path string) ([]byte, error)
}
```

## SQL 执行驱动（Driver）

### SqlDriver

包装 `*sql.DB`：

```go
driver := dbfly.NewSqlDriver(db)
err := driver.Execute(ctx, "SELECT 1")
```

### 自定义驱动

实现 `Driver` 接口：

```go
type Driver interface {
    Execute(ctx context.Context, sql string, args ...interface{}) error
    Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
    BeginTx(ctx context.Context) (Tx, error)
}
```

## 迁移器（Migratory）

### 内置迁移器

| 迁移器 | 创建方法 | 适用数据库 |
|--------|---------|-----------|
| MysqlMigratory | `NewMysqlMigratory()` | MySQL |
| SqliteMigratory | `NewSqliteMigratory()` | SQLite |
| PostgresMigratory | `NewPostgresMigratory()` | PostgreSQL |
| OracleMigratory | `NewOracleMigratory()` | Oracle |
| DamengMigratory | `NewDamengMigratory()` | DaMeng |
| VastbaseMigratory | `NewVastbaseMigratory()` | Vastbase |

### 自定义迁移器

实现 `Migratory` 接口，或嵌入 `DefaultMigratory` 覆盖特定方法：

```go
type Migratory interface {
    Name() string
    CreateTable(ctx, Driver, tableName, comment, columns, attributes) error
    CreateIndex(ctx, Driver, tableName, indexName, unique, columns, attributes) error
    // ... 其他 DDL 方法
    Script(ctx, Driver, script string) error
    SplitSQLStatements(script string) []string
    MetaData() DatabaseMetaData
}
```

## Dbfly 配置选项

```go
fly := dbfly.NewDbfly(
    migratory,
    driver,
    source,
    dbfly.WithEntrypoint("changelog.xml"),           // 自定义入口文件
    dbfly.WithLocker(dbfly.NewDbLocker(              // 自定义锁
        dbfly.WithLockerTableName("MY_LOCK"),
        dbfly.WithLockTimeout(60*time.Second),
    )),
    dbfly.WithRecorder(dbfly.NewDbRecorder(          // 自定义记录器
        dbfly.WithRecorderTableName("MY_LOG"),
    )),
)
```

---

# 高级特性

## 上下文支持

支持超时控制和取消：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err := fly.MigrateContext(ctx)
```

## 元数据查询

通过迁移器获取数据库信息：

```go
meta := migratory.MetaData()

// 判断表是否存在（返回实际名称，大小写不敏感）
exists, actualName, err := meta.ExistsTable(ctx, driver, "users")

// 获取所有表
tables, err := meta.GetTables(ctx, driver)

// 获取表的列
columns, err := meta.GetColumns(ctx, driver, "users")

// 获取索引
indexes, err := meta.GetIndexes(ctx, driver, "users")

// 获取主键
pks, err := meta.GetPrimaryKeys(ctx, driver, "users")
```

## 引号策略

不同数据库标识符引号：

| 数据库 | 引号 |
|--------|------|
| MySQL, SQLite | `` ` `` 反引号 |
| PostgreSQL, Oracle, DaMeng, Vastbase | `"` 双引号 |

迁移器自动处理，确保 SQL 符合目标数据库规范。

## SQL 拆分策略

`SplitSQLStatements` 将脚本拆分为执行单元：

- 分号 `;` 分隔
- 正确处理字符串（`''` 转义）
- 正确处理注释（`--` 行注释）
- 支持 MySQL `DELIMITER` 语法

---

# 附录

## XML Schema 约束

Schema 文件：`dbfly.xsd`

命名空间：`https://www.jianggujin.com/c/xml/dbfly`

IDE 配置后可获得自动补全和格式验证。

## API 参考

### Dbfly

```go
// 创建实例
NewDbfly(migratory, driver, source, opts...) *Dbfly

// 配置选项
WithEntrypoint(entrypoint string) DbflyOption
WithLocker(locker Locker) DbflyOption
WithRecorder(recorder Recorder) DbflyOption

// 执行迁移
Migrate() error
MigrateContext(ctx context.Context) error

// 访问组件
Migratory() Migratory
Driver() Driver
Source() Source
```

### Source

```go
// 创建数据源
NewEmbedFSSource(efs ...embed.FS) *FSSource
NewLocalFSSource(dirs ...string) *FSSource
NewFSSource(fsys ...fs.FS) *FSSource

// 接口方法
Read(path string) ([]byte, error)
```

### Driver

```go
// 创建驱动
NewSqlDriver(db *sql.DB) *SqlDriver

// 接口方法
Execute(ctx, sql, args...) error
Query(ctx, sql, args...) (Rows, error)
BeginTx(ctx) (Tx, error)
```

### Locker

```go
// 创建锁
NewDbLocker(opts...) *DbLocker

// 配置选项
WithLockerTableName(tableName string) LockerOption
WithLockRetryInterval(d time.Duration) LockerOption
WithLockTimeout(d time.Duration) LockerOption
WithLockMaxRetries(n int) LockerOption

// 接口方法
Lock(ctx, fly) (Unlock, error)
```

### Recorder

```go
// 创建记录器
NewDbRecorder(opts...) *DbRecorder

// 配置选项
WithRecorderTableName(tableName string) RecorderOption

// 接口方法
InitChangeLogTable(ctx, fly) error
GetExecutedChangeSets(ctx, fly) (map[string]bool, error)
NewChangeLog(ctx, fly, id, author, filename, order) error
CompleteChangeLog(ctx, fly, id) error
```

## 附录：数据库函数对照表

在 DML 操作中，使用 `originValue` 属性可以插入数据库函数表达式。不同数据库的函数名可能不同，以下对照表供参考。

**使用示例**：
```xml
<insert tableName="users">
    <column name="id" value="1"/>
    <column name="created_at" originValue="NOW()"/>  <!-- MySQL -->
</insert>
```

### 时间函数

| 功能 | MySQL | PostgreSQL | Oracle | SQLite | DaMeng | VastBase |
|------|-------|------------|--------|--------|--------|----------|
| 当前时间戳 | `NOW()` | `CURRENT_TIMESTAMP` | `SYSDATE` | `datetime('now')` | `SYSDATE` | `CURRENT_TIMESTAMP` |
| 当前日期 | `CURDATE()` | `CURRENT_DATE` | `TRUNC(SYSDATE)` | `date('now')` | `TRUNC(SYSDATE)` | `CURRENT_DATE` |
| 当前时间 | `CURTIME()` | `CURRENT_TIME` | `TO_CHAR(SYSDATE, 'HH24:MI:SS')` | `time('now')` | `TO_CHAR(SYSDATE, 'HH24:MI:SS')` | `CURRENT_TIME` |

### UUID 生成函数

| 功能 | MySQL | PostgreSQL | Oracle | SQLite | DaMeng | VastBase |
|------|-------|------------|--------|--------|--------|----------|
| 生成 UUID | `UUID()` | `gen_random_uuid()` | `SYS_GUID()` | `lower(hex(randomblob(16)))` | `SYS_GUID()` | `gen_random_uuid()` |

**注意**：SQLite 的 UUID 生成需要额外处理（如添加分隔符），上述表达式生成无分隔符的 32 位十六进制字符串。

## 许可证

Apache License 2.0