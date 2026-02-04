package dbfly

var OracleDataTypeMappers = map[string]string{
	Varchar:   "VARCHAR2",
	Char:      "CHAR",
	Text:      "CLOB",
	Clob:      "CLOB",
	Boolean:   "NUMBER(1)",
	Tinyint:   "NUMBER(3)",
	Smallint:  "NUMBER(5)",
	Int:       "NUMBER(10)",
	Bigint:    "NUMBER(19)",
	Decimal:   "NUMBER",
	Date:      "DATE",
	Time:      "TIMESTAMP",
	Timestamp: "TIMESTAMP",
	Blob:      "BLOB",
}

// OracleMigratory Oracle合并实现
type OracleMigratory struct {
	DefaultMigratory
}

// NewOracleMigratory 创建一个Oracle合并实现实例
func NewOracleMigratory() Migratory {
	showTablesSql := "SELECT TABLE_NAME FROM user_tables"
	return &OracleMigratory{
		DefaultMigratory{name: "oracle", showTablesSql: showTablesSql, dataTypeMapper: OracleDataTypeMappers},
	}
}
