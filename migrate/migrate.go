package migrate

import (
	"context"
	"fmt"
	"strings"

	_ "embed"

	"github.com/mlvzk/gopgs/internal/helper"

	"github.com/jackc/pgx/v4"
)

type Migration struct {
	Id         int
	Statements string
}

//go:embed migrate.sql
var migrateSql string

func Migrate(ctx context.Context, conn *pgx.Conn, schemaName string, migrations []Migration) error {
	if len(migrations) == 0 {
		return nil
	}

	_, err := conn.Exec(ctx, strings.ReplaceAll(migrateSql, "{{SchemaName}}", schemaName))
	if err != nil {
		return fmt.Errorf("failed to run migrate.sql: %w", err)
	}

	rows := make([][]interface{}, len(migrations))
	for i, m := range migrations {
		rows[i] = []interface{}{m.Id, m.Statements}
	}

	args, valuesSql := helper.GenerateValues(rows, []string{"integer", "text"})

	_, err = conn.Exec(ctx, `
	with vals as (
		values `+valuesSql+`
	)
	select pg_temp.migrate((select array_agg((vals.*)::pgqueue.done_migrations) from vals))
	`, args...)
	if err != nil {
		return err
	}

	return nil
}
