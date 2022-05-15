package helper

import (
	"reflect"
	"testing"
)

func TestGenerateWithStatement(t *testing.T) {
	type args[T any] struct {
		ts     []T
		tToRow func(*T, *ColumnSetter)
	}
	tests := []struct {
		name  string
		args  args[map[string]any]
		want  []any
		want1 string
	}{
		{
			name: "simple",
			args: args[map[string]any]{
				ts: []map[string]any{
					{
						"Id":   1,
						"Name": "test1",
					},
				},
				tToRow: func(t *map[string]any, x *ColumnSetter) {
					x.Set("Id", "int", (*t)["Id"])
					x.Set("Name", "text", (*t)["Name"])
				},
			},
			want:  []any{[]any{1}, []any{"test1"}},
			want1: "SELECT unnest($1::int[]) as Id, unnest($2::text[]) as Name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GenerateSelect(tt.args.ts, tt.args.tToRow)
			if got1 != tt.want1 {
				t.Errorf("GenerateWithStatement() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateWithStatement() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
