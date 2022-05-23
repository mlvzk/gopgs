package helper

import (
	"strconv"
	"strings"
)

type ColumnSetter struct {
	values    map[string]any
	types     map[string]string
	skipValue bool
}

func newColumnSetter(skipValue bool) *ColumnSetter {
	return &ColumnSetter{values: map[string]any{}, types: map[string]string{}, skipValue: skipValue}
}

func (c *ColumnSetter) Set(col string, sqlType string, val func() any) {
	if !c.skipValue {
		c.values[col] = val()
	}
	c.types[col] = sqlType
}

func GenerateSelect[T any](ts []T, tToRow func(*T, *ColumnSetter)) ([]any, string) {
	var withSql strings.Builder
	var args []any

	arrs := make(map[string][]any)
	var types map[string]string
	headerColumnSetter := newColumnSetter(true)
	tToRow(nil, headerColumnSetter)
	types = headerColumnSetter.types
	for colName := range types {
		arrs[colName] = []any{}
	}

	for _, t := range ts {
		cols := newColumnSetter(false)
		tToRow(&t, cols)
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
