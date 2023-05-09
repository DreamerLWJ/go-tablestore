package core

import "testing"

func TestSimpleEqQueryOptimizer_GeneratePlan(t *testing.T) {
	_ = []string{"sender", "receiver", "create_time", "msg_type", "cost"}

	tbInfo := TableInfo{
		TableName: "test-table",
		TableId:   1,
		Columns:   []ColumnInfo{},
		Indexs: []Index{{
			IndexId:     0,
			ColumnNames: []string{"receiver", "create_time"},
			Enable:      true,
		}, {
			IndexId:     0,
			ColumnNames: []string{"sender", "create_time"},
			Enable:      false,
		}, {
			IndexId:     0,
			ColumnNames: []string{"sender", "receiver", "create_time"},
			Enable:      false,
		}},
	}

	qr := NewSimpleEqQueryOptimizer()
	res := qr.GeneratePlan([]string{"sender", "receiver"}, tbInfo)
	t.Log(res)
}
