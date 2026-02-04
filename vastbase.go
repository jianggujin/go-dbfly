package dbfly

var VastbaseDataTypeMappers = map[string]string{
	Varchar:   "VARCHAR",
	Char:      "CHAR",
	Text:      "TEXT",
	Clob:      "TEXT",
	Boolean:   "SMALLINT",
	Tinyint:   "SMALLINT",
	Smallint:  "SMALLINT",
	Int:       "INTEGER",
	Bigint:    "BIGINT",
	Decimal:   "DECIMAL",
	Date:      "DATE",
	Time:      "TIME",
	Timestamp: "TIMESTAMP",
	Blob:      "BYTEA",
}

// VastbaseMigratory VastBase合并实现
type VastbaseMigratory struct {
	DefaultMigratory
}

// NewVastbaseMigratory 创建一个VastBase合并实现实例
func NewVastbaseMigratory() Migratory {
	showTablesSql := "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
	return &VastbaseMigratory{
		DefaultMigratory{name: "vastbase", showTablesSql: showTablesSql, dataTypeMapper: VastbaseDataTypeMappers},
	}
}
