package dbfly

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"regexp"
)

const defaultEntrypoint = "dbfly.xml"

type Dbfly struct {
	entrypoint string
	migratory  Migratory
	driver     Driver
	source     Source
	recorder   Recorder
	locker     Locker
	tx         Tx // 当前事务上下文
	logger     Logger
	logSQL     bool
}

type DbflyOption func(*Dbfly)

func WithEntrypoint(entrypoint string) DbflyOption {
	return func(db *Dbfly) {
		db.entrypoint = entrypoint
	}
}

func WithRecorder(recorder Recorder) DbflyOption {
	return func(db *Dbfly) {
		db.recorder = recorder
	}
}

func WithLocker(locker Locker) DbflyOption {
	return func(db *Dbfly) {
		db.locker = locker
	}
}

func WithLogger(logger Logger) DbflyOption {
	return func(db *Dbfly) {
		db.logger = logger
	}
}

func WithLogSQL(logSQL bool) DbflyOption {
	return func(db *Dbfly) {
		db.logSQL = logSQL
	}
}

func NewDbfly(migratory Migratory, driver Driver, source Source, opts ...DbflyOption) *Dbfly {
	fly := &Dbfly{
		migratory: migratory,
		driver:    driver,
		source:    source,
		logger:    nopLogger{},
		logSQL:    false,
	}
	for _, opt := range opts {
		opt(fly)
	}
	if fly.entrypoint == "" {
		fly.entrypoint = defaultEntrypoint
	}
	if fly.recorder == nil {
		fly.recorder = NewDbRecorder()
	}
	if fly.locker == nil {
		fly.locker = NewDbLocker()
	}
	return fly
}

func (f *Dbfly) Migratory() Migratory {
	return f.migratory
}

func (f *Dbfly) Driver() Driver {
	return f.driver
}

// Execute 执行SQL（自动使用事务或直接执行）
func (f *Dbfly) Execute(ctx context.Context, sql string, args ...any) error {
	if f.tx != nil {
		return f.tx.Execute(ctx, sql, args...)
	}
	return f.driver.Execute(ctx, sql, args...)
}

func (f *Dbfly) Source() Source {
	return f.source
}

// Migrate 迁移操作
func (f *Dbfly) Migrate() error {
	return f.MigrateContext(context.Background())
}

func (f *Dbfly) MigrateContext(ctx context.Context) (err error) {
	// 同步日志配置到 Migratory
	if m, ok := f.migratory.(*DefaultMigratory); ok {
		m.SetLogger(f.logger)
	} else if m, ok := f.migratory.(interface{ SetLogger(Logger) }); ok {
		m.SetLogger(f.logger)
	}

	f.logger.Info("migration started", "entrypoint", f.entrypoint)

	var unlock Unlock
	defer func() {
		if r := recover(); r != nil {
			retErr, ok := r.(error)
			if !ok {
				err = New("panic: %v", r)
			} else {
				err = retErr
			}
		}

		if unlock != nil {
			_ = unlock(ctx, f)
			f.logger.Debug("lock released")
		}
	}()

	// 解析 changelog
	changeSets, err := f.parseChangelog(f.entrypoint, make(map[string]bool))
	if err != nil {
		return err
	}

	f.logger.Debug("changelog parsed", "changeSetCount", len(changeSets))

	// 检测重复 changeSet id
	changeSetIds := make(map[string]bool)
	for _, cs := range changeSets {
		if changeSetIds[cs.Id] {
			return New("duplicate changeSet id: %s", cs.Id)
		}
		changeSetIds[cs.Id] = true
	}

	// 获取锁
	if f.locker != nil {
		if unlock, err = f.locker.Lock(ctx, f); err != nil {
			return err
		}
		f.logger.Debug("lock acquired")
	}

	// 初始化变更记录表
	if err = f.recorder.InitChangeLogTable(ctx, f); err != nil {
		return err
	}

	// 获取已执行的 changeSet ID 集合
	executedChangeSets, err := f.recorder.GetExecutedChangeSets(ctx, f)
	if err != nil {
		return err
	}

	// 按顺序执行未执行的 changeSet
	orderExecuted := 0
	skippedCount := 0
	for _, cs := range changeSets {
		// 获取已执行的最大 orderExecuted
		if executedChangeSets[cs.Id] {
			skippedCount++
			continue
		}

		orderExecuted++
		f.logger.Info("changeSet executing", "id", cs.Id, "author", cs.Author)
		if err = f.executeChangeSet(ctx, cs, orderExecuted); err != nil && "SKIP" != cs.OnFail {
			return err
		}
		if err == nil {
			f.logger.Info("changeSet completed", "id", cs.Id)
		}
	}

	f.logger.Info("migration completed", "executed", orderExecuted, "skipped", skippedCount)
	return nil
}

func (f *Dbfly) executeChangeSet(ctx context.Context, cs ChangeSet, orderExecuted int) error {
	// 创建变更记录
	if err := f.recorder.NewChangeLog(ctx, f, cs.Id, cs.Author, cs.Filename, orderExecuted); err != nil {
		return err
	}

	// 执行所有 DDL
	for _, ddl := range cs.DDLs {
		if err := ddl.Execute(ctx, f); err != nil {
			return err
		}
	}

	// 完成变更记录
	return f.recorder.CompleteChangeLog(ctx, f, cs.Id)
}

// parseChangelog 递归解析 changelog 文件
func (f *Dbfly) parseChangelog(path string, visited map[string]bool) (ChangeSets, error) {
	// 循环引用检测
	if visited[path] {
		return nil, New("circular reference detected: %s", path)
	}
	visited[path] = true

	// 读取文件内容
	content, err := f.source.Read(path)
	if err != nil {
		return nil, err
	}

	// 解析 XML
	changeSets, err := f.parseXmlContent(path, content, visited)
	if err != nil {
		return nil, Wrap(err, "parse changelog %s failed", path)
	}

	return changeSets, nil
}

// parseXmlContent 解析 XML 内容
func (f *Dbfly) parseXmlContent(filename string, content []byte, visited map[string]bool) (ChangeSets, error) {
	decoder := xml.NewDecoder(bytes.NewReader(content))
	var changeSets ChangeSets
	var currentChangeSet *ChangeSet

	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch ele := token.(type) {
		case xml.StartElement:
			name := ele.Name.Local
			switch name {
			case "dbfly":
				// 根元素，继续解析子元素
			case "changeSet":
				// 解析 changeSet 元素
				cs := &ChangeSetNode{}
				if err = decoder.DecodeElement(cs, &ele); err != nil {
					return nil, err
				}
				// 验证 id 格式
				if !isValidChangeSetId(cs.Id) {
					return nil, New("invalid changeSet id format: %s (allowed: [a-zA-Z_\\-.]+)", cs.Id)
				}
				// 设置默认 onFail
				if cs.OnFail == "" {
					cs.OnFail = "HALT"
				}
				currentChangeSet = &ChangeSet{
					Id:       cs.Id,
					Author:   cs.Author,
					OnFail:   cs.OnFail,
					Filename: filename,
					DDLs:     cs.DDLs,
				}
				changeSets = append(changeSets, *currentChangeSet)
			case "include":
				// 解析 include 元素
				var include IncludeNode
				if err = decoder.DecodeElement(&include, &ele); err != nil {
					return nil, err
				}
				// 递归解析引用文件，内容插入到当前位置
				includedChangeSets, err := f.parseChangelog(include.File, visited)
				if err != nil {
					return nil, err
				}
				changeSets = append(changeSets, includedChangeSets...)
			default:
				// DDL 元素必须在 changeSet 内
				return nil, New("DDL element <%s> must be inside a changeSet element", name)
			}
		}
	}

	return changeSets, nil
}

// isValidChangeSetId 验证 changeSet id 格式
func isValidChangeSetId(id string) bool {
	if id == "" {
		return false
	}
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_\-\.]+$`, id)
	return matched
}

// isDDLElement 判断是否为有效的 DDL 元素
func isDDLElement(name string) bool {
	ddlElements := map[string]bool{
		"createTable":       true,
		"createIndex":       true,
		"createPrimaryKey":  true,
		"dropTable":         true,
		"dropIndex":         true,
		"addColumn":         true,
		"renameColumn":      true,
		"alterColumn":       true,
		"dropColumn":        true,
		"dropPrimaryKey":    true,
		"renameTable":       true,
		"alterTableComment": true,
		"sqlFile":           true,
		"insert":            true,
		"update":            true,
		"delete":            true,
		"sqlInline":         true,
		"transaction":       true,
	}
	return ddlElements[name]
}
