package helper

import (
	"strconv"
	"strings"
)

type ColumnSetter struct {
	values map[string]any
	types  map[string]string
}

func newColumnSetter() *ColumnSetter {
	return &ColumnSetter{values: map[string]any{}, types: map[string]string{}}
}

func (c *ColumnSetter) Set(col string, sqlType string, val any) {
	c.values[col] = val
	c.types[col] = sqlType
}

func GenerateSelect[T any](ts []T, tToRow func(*T, *ColumnSetter)) ([]any, string) {
	var withSql strings.Builder
	var args []any

	arrs := make(map[string][]any)
	var types map[string]string
	for _, t := range ts {
		cols := newColumnSetter()
		tToRow(&t, cols)
		types = cols.types
		for k := range cols.values {
			arrs[k] = append(arrs[k], cols.values[k])
		}
	}

	withSql.WriteString("SELECT ")
	first := true
	for k := range arrs {
		if first {
			first = false
		} else {
			withSql.WriteString(", ")
		}

		args = append(args, arrs[k])
		withSql.WriteString("unnest($" + strconv.Itoa(len(args)) + "::" + types[k] + "[]) as " + k)
	}

	return args, withSql.String()
}
