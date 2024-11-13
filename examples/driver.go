package examples

import (
	"context"
	"database/sql"
	"github.com/jianggujin/go-dbfly"
)

func SqlDriverExample() {
	var db *sql.DB
	sql := ""
	driver := dbfly.NewSqlDriver(db)
	err := driver.Execute(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}

func DryRunDriverExample() {
	sql := ""
	driver := dbfly.NewDryRunDriver()
	err := driver.Execute(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}
