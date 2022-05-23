package migrate

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	_ "embed"

	"github.com/mlvzk/gopgs/internal/helper"

	"github.com/jackc/pgx/v4"
)

type Migration struct {
	Version    int
	Statements string
}

//go:embed migrate.sql
var migrateSql string

func Migrate(ctx context.Context, conn *pgx.Conn, schemaName string, migrations []Migration) error {
	if len(migrations) == 0 {
		return nil
	}

	defer conn.Exec(ctx, "DROP FUNCTION pg_temp.migrate")

	_, err := conn.Exec(ctx, strings.ReplaceAll(migrateSql, "{{SchemaName}}", schemaName))
	if err != nil {
		return fmt.Errorf("failed to run migrate.sql: %w", err)
	}

	args, valsSelect := helper.GenerateSelect(migrations, func(migration *Migration, cs *helper.ColumnSetter) {
		cs.Set("version", "int", func() any { return migration.Version })
		cs.Set("statements", "text", func() any { return migration.Statements })
	})

	_, err = conn.Exec(ctx, `
	with vals as (
		`+valsSelect+`
	)
	select pg_temp.migrate((select array_agg((version, statements)::`+schemaName+`.done_migrations) from vals))
	`, args...)
	if err != nil {
		return err
	}

	return nil
}

func EmbedFsToMigrations(migrationFs embed.FS, dirname string) ([]Migration, error) {
	migrations := make([]Migration, 0)

	entries, err := migrationFs.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			panic("migration files can't be directories")
		}

		id, err := strconv.Atoi(strings.Split(entry.Name(), "-")[0])
		if err != nil {
			panic("migration files must be named like '<id>-<name>.sql'")
		}

		content, err := migrationFs.ReadFile(filepath.Join(dirname, entry.Name()))
		if err != nil {
			panic("failed to read migration file '" + entry.Name() + "'")
		}

		migrations = append(migrations, Migration{
			Version:    id,
			Statements: string(content),
		})
	}

	return migrations, nil
}
