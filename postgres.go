package dbfly

var PostgresSqlDataTypeMappers = map[string]string{
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

// PostgresMigratory Postgres合并实现
type PostgresMigratory struct {
	DefaultMigratory
}

// NewPostgresMigratory 创建一个Postgres合并实现实例
func NewPostgresMigratory() Migratory {
	showTablesSql := "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
	return &PostgresMigratory{
		DefaultMigratory{name: "postgres", showTablesSql: showTablesSql, dataTypeMapper: PostgresSqlDataTypeMappers},
	}
}
