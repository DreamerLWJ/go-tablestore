package core

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

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

func TestTableStore_CreateIndex(t *testing.T) {
	tableStore := getTestTableStore()
	err := tableStore.ClearAllData()
	assert.Nil(t, err)

	err = tableStore.CreateTable(_testTableName, []ColumnInfo{{
		ColumnName: "content",
		ColumnType: ColumnTypeString,
		Nullable:   false,
	}, {
		ColumnName: "sender",
		ColumnType: ColumnTypeInteger,
		Nullable:   false,
	}, {
		ColumnName: "receiver",
		ColumnType: ColumnTypeInteger,
		Nullable:   false,
	}, {
		ColumnName: "create_time",
		ColumnType: ColumnTypeString,
		Nullable:   false,
	}}, nil)

	assert.Nil(t, err)

	err = tableStore.CreateIndex(_testTableName, Index{
		IndexId:     1,
		ColumnNames: []string{"sender", "receiver", "create_time"},
		Enable:      true,
	})
	assert.Nil(t, err)

	err = tableStore.PrintAll("")
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		err := tableStore.SaveMessage(_testTableName, &Message{
			MsgID: uint64(i + 1),
			ColumnValues: map[string]string{
				"content":     fmt.Sprintf("测试消息：%d", i),
				"sender":      strconv.Itoa(10001 + i),
				"receiver":    strconv.Itoa(20001 + i),
				"create_time": time.Now().Add(time.Duration(i) * time.Hour).Format(_testDateTimeFormat),
			},
		})
		assert.Nil(t, err)
	}

	// Print msg
	err = tableStore.PrintAll("msg")
	assert.Nil(t, err)
	// Print idx
	err = tableStore.PrintAll("idx_val")
	assert.Nil(t, err)

	_, _, err = tableStore.QueryMessage(MessageQuery{
		TableName: _testTableName,
		MsgId:     0,
		Marker:    0,
		PageSize:  0,
		EqCond: []QueryCond{{
			ColumnName:  "sender",
			Oper:        Gt,
			ColumnValue: strconv.Itoa(10001 + 3),
		}, {
			ColumnName:  "receiver",
			Oper:        Gt,
			ColumnValue: strconv.Itoa(20001 + 4),
		}},
	})
	assert.Nil(t, err)
}
