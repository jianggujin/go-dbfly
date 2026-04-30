package dbfly

import (
	"context"
	"testing"
)

// TestExistsColumn 测试 ExistsColumn 函数
func TestExistsColumn(t *testing.T) {
	tests := []struct {
		name         string
		tableGetter  TableGetter
		columnGetter ColumnGetter
		tableName    string
		columnName   string
		wantExists   bool
		wantTable    string
		wantColumn   string
		wantErr      bool
	}{
		{
			name: "列存在 - 大小写不敏感匹配",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return []string{"ID", "NAME", "EMAIL"}, nil
			},
			tableName:  "users",
			columnName: "name",
			wantExists: true,
			wantTable:  "USERS",
			wantColumn: "NAME",
			wantErr:    false,
		},
		{
			name: "列不存在",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return []string{"ID", "NAME", "EMAIL"}, nil
			},
			tableName:  "USERS",
			columnName: "phone",
			wantExists: false,
			wantTable:  "",
			wantColumn: "",
			wantErr:    false,
		},
		{
			name: "表不存在",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{}, nil
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return []string{}, nil
			},
			tableName:  "USERS",
			columnName: "NAME",
			wantExists: false,
			wantTable:  "",
			wantColumn: "",
			wantErr:    false,
		},
		{
			name: "获取表失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return nil, New("database connection failed")
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return []string{}, nil
			},
			tableName:  "USERS",
			columnName: "NAME",
			wantExists: false,
			wantTable:  "",
			wantColumn: "",
			wantErr:    true,
		},
		{
			name: "获取列失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return nil, New("query failed")
			},
			tableName:  "USERS",
			columnName: "NAME",
			wantExists: false,
			wantTable:  "",
			wantColumn: "",
			wantErr:    true,
		},
		{
			name: "列名完全匹配（大小写相同）",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			columnGetter: func(ctx context.Context, driver Driver, tableName string) ([]string, error) {
				return []string{"ID", "NAME", "EMAIL"}, nil
			},
			tableName:  "USERS",
			columnName: "NAME",
			wantExists: true,
			wantTable:  "USERS",
			wantColumn: "NAME",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			driver := &SqlDriver{} // mock driver, 不实际使用

			gotExists, gotTable, gotColumn, err := ExistsColumn(tt.tableGetter, tt.columnGetter, ctx, driver, tt.tableName, tt.columnName)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExistsColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotExists != tt.wantExists {
				t.Errorf("ExistsColumn() exists = %v, want %v", gotExists, tt.wantExists)
			}

			if gotTable != tt.wantTable {
				t.Errorf("ExistsColumn() table = %v, want %v", gotTable, tt.wantTable)
			}

			if gotColumn != tt.wantColumn {
				t.Errorf("ExistsColumn() column = %v, want %v", gotColumn, tt.wantColumn)
			}
		})
	}
}

// TestExistsIndex 测试 ExistsIndex 函数
func TestExistsIndex(t *testing.T) {
	tests := []struct {
		name        string
		tableGetter TableGetter
		indexGetter IndexGetter
		tableName   string
		indexName   string
		wantExists  bool
		wantTable   string
		wantIndex   string
		wantErr     bool
	}{
		{
			name: "索引存在 - 大小写不敏感匹配",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return []*Index{
					{Name: "IDX_NAME", ColumnName: "NAME"},
					{Name: "IDX_EMAIL", ColumnName: "EMAIL"},
				}, nil
			},
			tableName:  "users",
			indexName:  "idx_name",
			wantExists: true,
			wantTable:  "USERS",
			wantIndex:  "IDX_NAME",
			wantErr:    false,
		},
		{
			name: "索引不存在",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return []*Index{
					{Name: "IDX_NAME", ColumnName: "NAME"},
					{Name: "IDX_EMAIL", ColumnName: "EMAIL"},
				}, nil
			},
			tableName:  "USERS",
			indexName:  "idx_phone",
			wantExists: false,
			wantTable:  "",
			wantIndex:  "",
			wantErr:    false,
		},
		{
			name: "获取索引失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return nil, New("index query failed")
			},
			tableName:  "USERS",
			indexName:  "IDX_NAME",
			wantExists: false,
			wantTable:  "",
			wantIndex:  "",
			wantErr:    true,
		},
		{
			name: "获取表失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return nil, New("table query failed")
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return []*Index{}, nil
			},
			tableName:  "USERS",
			indexName:  "IDX_NAME",
			wantExists: false,
			wantTable:  "",
			wantIndex:  "",
			wantErr:    true,
		},
		{
			name: "索引名完全匹配（大小写相同）",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return []*Index{
					{Name: "IDX_NAME", ColumnName: "NAME"},
					{Name: "IDX_EMAIL", ColumnName: "EMAIL"},
				}, nil
			},
			tableName:  "USERS",
			indexName:  "IDX_NAME",
			wantExists: true,
			wantTable:  "USERS",
			wantIndex:  "IDX_NAME",
			wantErr:    false,
		},
		{
			name: "空索引列表",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			indexGetter: func(ctx context.Context, driver Driver, tableName string) ([]*Index, error) {
				return []*Index{}, nil
			},
			tableName:  "USERS",
			indexName:  "IDX_NAME",
			wantExists: false,
			wantTable:  "",
			wantIndex:  "",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			driver := &SqlDriver{} // mock driver, 不实际使用

			gotExists, gotTable, gotIndex, err := ExistsIndex(tt.tableGetter, tt.indexGetter, ctx, driver, tt.tableName, tt.indexName)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExistsIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotExists != tt.wantExists {
				t.Errorf("ExistsIndex() exists = %v, want %v", gotExists, tt.wantExists)
			}

			if gotTable != tt.wantTable {
				t.Errorf("ExistsIndex() table = %v, want %v", gotTable, tt.wantTable)
			}

			if gotIndex != tt.wantIndex {
				t.Errorf("ExistsIndex() index = %v, want %v", gotIndex, tt.wantIndex)
			}
		})
	}
}

// TestExistsPrimaryKey 测试 ExistsPrimaryKey 函数
func TestExistsPrimaryKey(t *testing.T) {
	tests := []struct {
		name        string
		tableGetter TableGetter
		pkGetter    PrimaryKeyGetter
		tableName   string
		wantExists  bool
		wantTable   string
		wantErr     bool
	}{
		{
			name: "主键存在",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return []*PrimaryKey{
					{Name: "PK_USERS", ColumnName: "ID"},
				}, nil
			},
			tableName:  "USERS",
			wantExists: true,
			wantTable:  "USERS",
			wantErr:    false,
		},
		{
			name: "主键不存在（空列表）",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return []*PrimaryKey{}, nil
			},
			tableName:  "USERS",
			wantExists: false,
			wantTable:  "USERS", // 注意：即使没有主键，也返回实际表名
			wantErr:    false,
		},
		{
			name: "获取主键失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return nil, New("primary key query failed")
			},
			tableName:  "USERS",
			wantExists: false,
			wantTable:  "",
			wantErr:    true,
		},
		{
			name: "获取表失败",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return nil, New("table query failed")
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return []*PrimaryKey{}, nil
			},
			tableName:  "USERS",
			wantExists: false,
			wantTable:  "",
			wantErr:    true,
		},
		{
			name: "复合主键存在",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "ORDER_ITEMS"}}, nil
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return []*PrimaryKey{
					{Name: "PK_ORDER_ITEMS", ColumnName: "ORDER_ID"},
					{Name: "PK_ORDER_ITEMS", ColumnName: "PRODUCT_ID"},
				}, nil
			},
			tableName:  "ORDER_ITEMS",
			wantExists: true,
			wantTable:  "ORDER_ITEMS",
			wantErr:    false,
		},
		{
			name: "表名大小写不敏感",
			tableGetter: func(ctx context.Context, driver Driver) ([]*Table, error) {
				return []*Table{{Name: "USERS"}}, nil
			},
			pkGetter: func(ctx context.Context, driver Driver, tableName string) ([]*PrimaryKey, error) {
				return []*PrimaryKey{
					{Name: "PK_USERS", ColumnName: "ID"},
				}, nil
			},
			tableName:  "users",
			wantExists: true,
			wantTable:  "USERS",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			driver := &SqlDriver{} // mock driver, 不实际使用

			gotExists, gotTable, err := ExistsPrimaryKey(tt.tableGetter, tt.pkGetter, ctx, driver, tt.tableName)

			if (err != nil) != tt.wantErr {
				t.Errorf("ExistsPrimaryKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotExists != tt.wantExists {
				t.Errorf("ExistsPrimaryKey() exists = %v, want %v", gotExists, tt.wantExists)
			}

			if gotTable != tt.wantTable {
				t.Errorf("ExistsPrimaryKey() table = %v, want %v", gotTable, tt.wantTable)
			}
		})
	}
}
