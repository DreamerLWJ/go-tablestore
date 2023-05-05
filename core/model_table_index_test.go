package core

import "testing"

func TestGenerateIndexKey(t *testing.T) {
	key := GenerateIndexKey(1, map[string]ColumnInfo{
		"create_time": {
			ColumnName: "create_time",
			ColumnType: ColumnTypeString,
			Nullable:   false,
		},
	}, Index{
		IndexId:     1,
		ColumnNames: []string{"create_time"},
		Enable:      true,
	}, &Message{
		MsgID: 1,
		ColumnValues: map[string]string{
			"create_time": "2023-05-03 18:25:00",
		},
	})
	t.Log(key)
}
