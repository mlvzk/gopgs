package helper

import (
	"strconv"
	"strings"
)

func GenerateValues(rows [][]any, types []string) ([]any, string) {
	var valuesSql strings.Builder
	var args []any

	for i, row := range rows {
		if i != 0 {
			valuesSql.WriteString(", ")
		}
		valuesSql.WriteString("(")
		for i := 0; i < len(row); i++ {
			if i != 0 {
				valuesSql.WriteString(", ")
			}
			valuesSql.WriteString("$" + strconv.Itoa(len(args)+1+i) + "::" + types[i])
		}
		args = append(args, row...)
		valuesSql.WriteString(")")
	}

	return args, valuesSql.String()
}
