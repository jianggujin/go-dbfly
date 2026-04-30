package dbfly

import (
	"testing"
)

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name: "标准DDL脚本",
			input: `CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
CREATE INDEX idx_name ON users(name);`,
			expected: []string{
				"CREATE TABLE users (\n    id INT PRIMARY KEY,\n    name VARCHAR(100)\n)",
				"CREATE INDEX idx_name ON users(name)",
			},
		},
		{
			name:     "标准SQL转义单引号",
			input:    `INSERT INTO messages (text) VALUES ('It''s a test');SELECT 'O''Brien' AS name;`,
			expected: []string{"INSERT INTO messages (text) VALUES ('It''s a test')", "SELECT 'O''Brien' AS name"},
		},
		{
			name:     "MySQL风格反斜杠转义",
			input:    `INSERT INTO messages (text) VALUES ('It\'s a test');SELECT 'O\'Brien' AS name;`,
			expected: []string{`INSERT INTO messages (text) VALUES ('It\'s a test')`, `SELECT 'O\'Brien' AS name`},
		},
		{
			name: "MySQL DELIMITER存储过程",
			input: `DELIMITER $$

CREATE PROCEDURE test_proc()
BEGIN
    SELECT * FROM users;
    INSERT INTO logs VALUES (1);
END $$

DELIMITER ;`,
			expected: []string{
				"DELIMITER $$",
				"CREATE PROCEDURE test_proc()\nBEGIN\n    SELECT * FROM users;\n    INSERT INTO logs VALUES (1);\nEND",
				"DELIMITER ;",
			},
		},
		{
			name:     "注释中的分号",
			input:    "-- 这是注释; 不应该拆分\nSELECT 1;",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "空脚本",
			input:    "",
			expected: nil,
		},
		{
			name:     "只有注释",
			input:    "-- 只有注释\n-- 另一行注释",
			expected: nil,
		},
		{
			name:     "双引号标识符",
			input:    `SELECT "column;name" FROM "table;name";`,
			expected: []string{`SELECT "column;name" FROM "table;name"`},
		},
		{
			name:  "混合场景",
			input: "CREATE TABLE test (id INT);-- 注释\nINSERT INTO test VALUES (1);DELIMITER $$\nSELECT * FROM test$$DELIMITER ;",
			expected: []string{
				"CREATE TABLE test (id INT)",
				"INSERT INTO test VALUES (1)",
				"DELIMITER $$",
				"SELECT * FROM test",
				"DELIMITER ;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitSQLStatements(tt.input)

			// 处理 nil vs empty 切片
			if tt.expected == nil {
				if result != nil {
					t.Errorf("splitSQLStatements() = %v, want nil", result)
				}
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("splitSQLStatements() len = %d, want %d\nresult = %v", len(result), len(tt.expected), result)
				return
			}

			for i, stmt := range result {
				if stmt != tt.expected[i] {
					t.Errorf("splitSQLStatements()[%d] = %q, want %q", i, stmt, tt.expected[i])
				}
			}
		})
	}
}
