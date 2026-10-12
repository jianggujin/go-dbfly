package dbfly

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Binder interface {
	// Rebind 对指定SQL进行动态参数重绑定
	Rebind(sql string) string
	// Placeholder 指定索引位置占位符，索引从1开始
	Placeholder(pos int) string
}

var (
	// Question 默认，使用?作为占位符
	Question = questionBinder{}

	// Dollar 使用$前缀作为占位符，例如：$1, $2, $3
	Dollar = &defaultBinder{prefix: "$"}

	// Colon 使用:前缀作为占位符，例如：:1, :2, :3
	Colon = &defaultBinder{prefix: ":"}

	// AtP 使用@p前缀作为占位符，例如：@p1, @p2, @p3
	AtP = &defaultBinder{prefix: "@p"}
)

type questionBinder struct{}

func (questionBinder) Rebind(sql string) string {
	return sql
}
func (questionBinder) Placeholder(pos int) string {
	return "?"
}

type defaultBinder struct {
	prefix string
}

func (b *defaultBinder) Rebind(sql string) string {
	return replacePositionalPlaceholders(sql, b.prefix)
}
func (b *defaultBinder) Placeholder(pos int) string {
	return b.prefix + strconv.Itoa(pos)
}

func replacePositionalPlaceholders(sql, prefix string) string {
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			i++
			buf.WriteString(sql[:p])
			_, _ = fmt.Fprintf(buf, "%s%d", prefix, i)
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String()
}

func DefaultBinder(driverName string) Binder {
	switch driverName {
	case "postgres", "pgx", "pq-timeouts", "cloudsqlpostgres", "ql", "nrpostgres", "cockroach":
		return Dollar
	case "sqlserver", "azuresql":
		return AtP
	case "oracle", "godror":
		return Colon
	case "mysql", "sqlite3", "nrmysql", "nrsqlite3", "sqlite":
		fallthrough
	default:
		return Question
	}
}
