package dbfly

import (
	"fmt"
	"strings"
)

// 将SQL脚本拆分为单个最小执行单元脚本
func SplitSQLStatements(sqlText string) []string {
	var statements []string
	var currentStmt strings.Builder
	var inString, inComment bool
	var quoteCounter int

	addStatement := func() {
		if currentStmt.Len() != 0 {
			statement := strings.TrimSpace(currentStmt.String())
			if statement != "" {
				statements = append(statements, statement)
			}
		}
		currentStmt.Reset()
	}
	slice := []rune(sqlText)
	sliceLen := len(slice)
	for pos := 0; pos < sliceLen; pos++ {
		char := slice[pos]
		if char == ';' && !inString && !inComment && quoteCounter%2 == 0 {
			addStatement()
		} else {
			skip := false
			switch char {
			case ';': // 分隔符，判断是否为一段SQL结束
				if !inString && !inComment && quoteCounter%2 == 0 {
					addStatement()
				}
			case '\'':
				if !inComment {
					inString = !inString
				}
			case '"':
				if !inComment && !inString {
					quoteCounter++
				}
			case '-':
				if !inString && !inComment {
					// 判断后续一位是否为-，如果是则表示注释开始
					if pos+1 < sliceLen {
						nextChar := slice[pos+1]
						if nextChar == '-' {
							pos++
							inComment = true
						}
					}
				}
			case '\r':
				skip = true
			case '\n':
				if inComment {
					inComment = false
					skip = true
				}
			}

			// 需要跳过或者当前为注释行，则跳过
			if skip || inComment {
				continue
			}
			currentStmt.WriteRune(char)
		}
	}
	// 添加剩余的部分
	addStatement()

	return statements
}

// 针对字符串、DECIMAL等数据类型添加长度约束
func ColumnType(dataType, columnType string, maxLength, numericScale int) string {
	switch dataType {
	case Varchar, Char:
		return fmt.Sprintf("%s(%d)", columnType, maxLength)
	case Decimal:
		if numericScale > 0 {
			return fmt.Sprintf("%s(%d, %d)", columnType, maxLength, numericScale)
		}
		return fmt.Sprintf("%s(%d)", columnType, maxLength)
	default:
		return columnType
	}
}
