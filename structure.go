package dbfly

import (
	"fmt"
	"sort"
	"strings"
)

type Structure struct {
	entrypoint string
	source     Source
}

func NewStructure(source Source, entrypoints ...string) *Structure {
	s := &Structure{
		source: source,
	}
	if len(entrypoints) > 0 {
		s.entrypoint = entrypoints[0]
	} else {
		s.entrypoint = defaultEntrypoint
	}
	return s
}

func (s *Structure) Source() Source {
	return s.source
}

func (s *Structure) Parse() (info *StructureInfo, err error) {
	defer func() {
		r := recover()
		if r != nil {
			if rErr, ok := r.(error); ok {
				err = rErr
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	// 解析 changelog
	changeSets, err := parseChangelog(s.source, s.entrypoint, make(map[string]bool))
	if err != nil {
		return nil, err
	}

	// 检测重复 changeSet id
	changeSetIds := make(map[string]bool)
	for _, cs := range changeSets {
		if changeSetIds[cs.Id] {
			return nil, New("duplicate changeSet id: %s", cs.Id)
		}
		changeSetIds[cs.Id] = true
	}

	info = &StructureInfo{
		tables: make(map[string]*TableInfo),
	}
	for _, cs := range changeSets {
		for _, ddl := range cs.DDLs {
			if parser, ok := ddl.(Parser); ok {
				if err = parser.Parse(info); err != nil {
					return nil, err
				}
			}
		}
	}

	return info, nil
}

type StructureInfo struct {
	tables map[string]*TableInfo
}

func (s *StructureInfo) MustTable(name string) *TableInfo {
	table, _ := s.FindTable(name)
	if table == nil {
		panic("table not found: " + name)
	}
	return table
}

func (s *StructureInfo) FindTable(name string) (*TableInfo, bool) {
	name = strings.ToLower(name)
	table, ok := s.tables[name]
	return table, ok
}

func (s *StructureInfo) SetTable(table *TableInfo) *TableInfo {
	name := strings.ToLower(table.TableName)
	s.tables[name] = table
	return table
}

func (s *StructureInfo) RemoveTable(name string) {
	delete(s.tables, strings.ToLower(name))
}

func (s *StructureInfo) Tables() []*TableInfo {
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	tables := make([]*TableInfo, 0, len(names))
	for _, name := range names {
		tables = append(tables, s.tables[name])
	}
	return tables
}

type TableInfo struct {
	TableName string
	Comment   string
	Columns   []*ColumnInfo
	Indexes   []*IndexInfo
}

func (t *TableInfo) FindIndex(name string) (*IndexInfo, bool) {
	for _, index := range t.Indexes {
		if strings.EqualFold(index.IndexName, name) {
			return index, true
		}
	}
	return nil, false
}

func (t *TableInfo) RemoveIndex(name string) (*IndexInfo, bool) {
	for i, index := range t.Indexes {
		if strings.EqualFold(index.IndexName, name) {
			t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
			return index, true
		}
	}
	return nil, false
}

func (t *TableInfo) FindColumn(name string) (*ColumnInfo, bool) {
	for _, column := range t.Columns {
		if strings.EqualFold(column.ColumnName, name) {
			return column, true
		}
	}
	return nil, false
}

func (t *TableInfo) RemoveColumn(name string) (*ColumnInfo, bool) {
	for i, column := range t.Columns {
		if strings.EqualFold(column.ColumnName, name) {
			t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
			return column, true
		}
	}
	return nil, false
}

type ColumnInfo struct {
	ColumnName   string
	DataType     string
	MaxLength    int
	NumericScale int
	Notnull      bool
	Unique       bool
	PrimaryKey   bool
	KeyName      string
	DefaultValue string
	Comment      string
}

type IndexInfo struct {
	IndexName string
	Unique    bool
	Columns   []string
}
