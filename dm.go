package dbfly

var DmDataTypeMappers = map[string]string{
	Varchar:   "VARCHAR",
	Char:      "CHAR",
	Text:      "TEXT",
	Clob:      "CLOB",
	Boolean:   "BOOLEAN",
	Tinyint:   "TINYINT",
	Smallint:  "SMALLINT",
	Int:       "INT",
	Bigint:    "BIGINT",
	Decimal:   "DECIMAL",
	Date:      "DATE",
	Time:      "TIME",
	Timestamp: "TIMESTAMP",
	Blob:      "BLOB",
}

// DmMigratory 达梦合并实现
type DmMigratory struct {
	DefaultMigratory
}

// NewDmMigratory 创建一个达梦合并实现实例
func NewDmMigratory() Migratory {
	showTablesSql := "SELECT TABLE_NAME FROM user_tables"
	return &DmMigratory{
		DefaultMigratory{name: "dm", showTablesSql: showTablesSql, dataTypeMapper: DmDataTypeMappers},
	}
}
