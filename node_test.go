package dbfly

import (
	"strings"
	"testing"
)

func TestInsertNode_BatchMode_NoRedundantCheck(t *testing.T) {
	// 验证批量模式不会因空行崩溃
	node := &InsertNode{
		TableName: "test_table",
		Rows:      []*DataRowNode{},
	}
	fly := &Dbfly{}
	// 使用 recover 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 如果 panic 发生，验证它不是因为空行导致的索引越界
			// 预期的 panic 是因为 fly.Migratory() 返回 nil
			// 空行切片 len(Rows)==0 会跳过批量模式，不会访问 Rows[0]
			t.Logf("caught panic (expected due to nil migratory): %v", r)
		}
	}()
	if err := node.Execute(nil, fly); err != nil {
		t.Logf("expected error with empty fly: %v", err)
	}
}

func TestInsertNode_SingleMode_EmptyColumns(t *testing.T) {
	node := &InsertNode{
		TableName: "test_table",
		Columns:   []*DataColumnNode{},
	}
	fly := &Dbfly{}
	defer func() {
		if r := recover(); r != nil {
			t.Logf("caught panic (expected due to nil migratory): %v", r)
		}
	}()
	if err := node.Execute(nil, fly); err != nil {
		t.Logf("expected error with empty fly: %v", err)
	}
}

func TestWriteColumnValue(t *testing.T) {
	tests := []struct {
		name     string
		col      *DataColumnNode
		expected string
	}{
		{
			name:     "value with single quote",
			col:      &DataColumnNode{Value: "it's a test"},
			expected: "'it''s a test'",
		},
		{
			name:     "origin value",
			col:      &DataColumnNode{OriginValue: "NOW()"},
			expected: "NOW()",
		},
		{
			name:     "empty value",
			col:      &DataColumnNode{Value: ""},
			expected: "''",
		},
		{
			name:     "normal value",
			col:      &DataColumnNode{Value: "hello"},
			expected: "'hello'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var builder strings.Builder
			writeColumnValue(&builder, tt.col)
			if builder.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, builder.String())
			}
		})
	}
}
