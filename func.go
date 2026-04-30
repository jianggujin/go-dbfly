package dbfly

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// 将SQL脚本拆分为单个最小执行单元脚本
func splitSQLStatements(sqlText string) []string {
	var statements []string
	var currentStmt strings.Builder
	var inString, inComment bool
	var quoteCounter int
	defaultDelimiter := ';'
	currentDelimiter := defaultDelimiter
	var inDelimiterStmt bool
	var delimiterBuf strings.Builder

	addStatement := func() {
		if currentStmt.Len() != 0 {
			statement := strings.TrimSpace(currentStmt.String())
			if statement != "" {
				statements = append(statements, statement)
			}
		}
		currentStmt.Reset()
	}

	addDelimiterStatement := func() {
		if delimiterBuf.Len() > 0 {
			stmt := strings.TrimSpace(delimiterBuf.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
		}
		delimiterBuf.Reset()
	}

	slice := []rune(sqlText)
	sliceLen := len(slice)
	pos := 0

	for pos < sliceLen {
		char := slice[pos]

		// 检测 DELIMITER 语句（只在不在字符串/注释/引号内时检测）
		if !inString && !inComment && quoteCounter%2 == 0 {
			// 检查是否开始 DELIMITER 语句
			if !inDelimiterStmt && (char == 'D' || char == 'd') {
				// 尝试匹配 "DELIMITER" 关键字（9个字符）
				if pos+8 < sliceLen {
					matched := true
					expected := []rune{'E', 'L', 'I', 'M', 'I', 'T', 'E', 'R'}
					for i, e := range expected {
						c := slice[pos+1+i]
						if c != e && c != e+32 { // 大小写不敏感
							matched = false
							break
						}
					}
					if matched {
						// 匹配到 DELIMITER 关键字，写入完整的 DELIMITER
						inDelimiterStmt = true
						for i := 0; i < 9; i++ {
							delimiterBuf.WriteRune(slice[pos+i])
						}
						pos += 9 // 跳过整个 DELIMITER 关键字
						continue
					}
				}
			}

			// 在 DELIMITER 语句中，寻找新的分隔符
			if inDelimiterStmt {
				if char == '\n' || char == '\r' {
					// DELIMITER 语句结束，提取新的分隔符
					delimiterStr := delimiterBuf.String()
					// 去掉 "DELIMITER"（9个字符）
					if len(delimiterStr) > 9 {
						newDelimiterStr := strings.TrimSpace(delimiterStr[9:])
						if len(newDelimiterStr) > 0 {
							delimiterRunes := []rune(newDelimiterStr)
							if len(delimiterRunes) > 0 {
								currentDelimiter = delimiterRunes[0]
							}
						}
					}
					addDelimiterStatement()
					addStatement() // DELIMITER 语句本身也加入
					inDelimiterStmt = false
					delimiterBuf.Reset()
				} else {
					delimiterBuf.WriteRune(char)
				}
				// 跳过 `\r\n` 中的 `\r`
				if char == '\r' && pos+1 < sliceLen && slice[pos+1] == '\n' {
					pos++
				}
				pos++
				continue
			}

			// 使用动态分隔符判断语句结束
			if char == currentDelimiter {
				addStatement()
				pos++
				continue
			}
		}

		// 处理 MySQL 风格反斜杠转义（在字符串内）
		if char == '\\' && inString && !inComment {
			// 检查是否是转义序列
			if pos+1 < sliceLen {
				nextChar := slice[pos+1]
				// MySQL 转义：\' 表示单引号，\\ 表示反斜杠
				currentStmt.WriteRune(char) // 写入反斜杠
				pos++
				currentStmt.WriteRune(nextChar) // 写入被转义的字符
				pos++
				continue
			}
		}

		// 处理单引号（字符串边界和标准SQL转义）
		if char == '\'' && !inComment {
			// 检查标准 SQL 转义：''
			if pos+1 < sliceLen && slice[pos+1] == '\'' {
				// 标准SQL转义：''
				currentStmt.WriteRune(char)
				pos++
				currentStmt.WriteRune(slice[pos])
				pos++
				continue
			}
			// 切换字符串状态
			inString = !inString
		}

		// 原有逻辑处理其他字符
		skip := false
		switch char {
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
			pos++
			continue
		}
		currentStmt.WriteRune(char)
		pos++
	}
	// 添加剩余的部分
	if inDelimiterStmt {
		// DELIMITER 语句未正常结束
		delimiterStr := delimiterBuf.String()
		if len(delimiterStr) > 9 {
			newDelimiterStr := strings.TrimSpace(delimiterStr[9:])
			if len(newDelimiterStr) > 0 {
				delimiterRunes := []rune(newDelimiterStr)
				if len(delimiterRunes) > 0 {
					currentDelimiter = delimiterRunes[0]
				}
			}
		}
		addDelimiterStatement()
		inDelimiterStmt = false
	}
	addStatement()

	return statements
}

// 针对字符串、DECIMAL等数据类型添加长度约束
func columnType(dataType, columnType string, maxLength, numericScale int) string {
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

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type Float interface {
	~float32 | ~float64
}

type Number interface {
	Signed | Unsigned | Float
}

var NoData = errors.New("no data")

func doGetScalar[T Number | string](ctx context.Context, driver Driver, sql string, args ...interface{}) (T, error) {
	return doGet[T](ctx, driver, func(rows Rows, t *T) error {
		return rows.Scan(t)
	}, sql, args...)
}

func doGetScalars[T Number | string](ctx context.Context, driver Driver, sql string, args ...interface{}) ([]T, error) {
	var values []T
	err := doEach(ctx, driver, func(rows Rows) error {
		var value T
		if err := rows.Scan(&value); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	}, sql, args...)
	return values, err
}

func doGet[T any](ctx context.Context, driver Driver, function func(Rows, *T) error, sql string, args ...interface{}) (T, error) {
	var value T
	rows, err := driver.Query(ctx, sql, args...)
	if err != nil {
		return value, err
	}
	defer rows.Close()
	if rows.Next() {
		err = function(rows, &value)
	} else {
		err = NoData
	}
	return value, err
}

func doGetSlices[T any](ctx context.Context, driver Driver, function func(Rows, *T) error, sql string, args ...interface{}) ([]*T, error) {
	var list []*T
	err := doEach(ctx, driver, func(rows Rows) error {
		item := new(T)
		if err := function(rows, item); err != nil {
			return err
		}
		list = append(list, item)
		return nil
	}, sql, args...)
	return list, err
}

func doEach(ctx context.Context, driver Driver, function func(Rows) error, sql string, args ...interface{}) error {
	rows, err := driver.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = function(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

type columnBinders map[string]interface{}

type scanPlan struct {
	scanArgs []interface{}
	dummies  []sql.RawBytes
}

func newScanPlan(rows Rows, binders columnBinders) (*scanPlan, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scanArgs := make([]interface{}, len(columns))
	dummies := make([]sql.RawBytes, len(columns))

	for i, col := range columns {
		col = strings.ToUpper(col)
		if dst, ok := binders[col]; ok {
			scanArgs[i] = dst
		} else {
			scanArgs[i] = &dummies[i]
		}
	}

	return &scanPlan{
		scanArgs: scanArgs,
		dummies:  dummies,
	}, nil
}

func (p *scanPlan) Scan(rows Rows) error {
	return rows.Scan(p.scanArgs...)
}
