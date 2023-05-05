package core

import (
	"github.com/stretchr/testify/assert"
	"go-tablestore/config"
	"testing"
	"time"
)

func getTestTableStore() *TableStore {
	db, err := NewTableStore(config.DBConfig{DataDir: "./data"})
	if err != nil {
		panic(err)
	}
	return db
}

func TestIMDB_PrintAll(t *testing.T) {
	imdb := getTestTableStore()
	defer imdb.CloseGracefully()

	err := imdb.PrintAll("")
	if err != nil {
		panic(err)
	}
}

func TestIMDB_ClearAllData(t *testing.T) {
	imdb := getTestTableStore()
	err := imdb.ClearAllData()
	if err != nil {
		panic(err)
	}
}

func TestTableStore_SaveMessage(t *testing.T) {
	tableStore := getTestTableStore()
	defer tableStore.CloseGracefully()
	err := tableStore.SaveMessage("test-table", &Message{
		MsgID: 1,
		ColumnValues: map[string]string{
			"create_time": time.Now().Format("2006-01-02 15:04:05"),
			"sender":      "100001",
			"receiver":    "100002",
			"msgType":     "5",
		},
	})
	assert.Nil(t, err)
}

func TestTableStore_QueryMessage(t *testing.T) {
	tableStore := getTestTableStore()
	defer tableStore.CloseGracefully()
	res, marker, err := tableStore.QueryMessage(MessageQuery{
		TableName: "test-table",
		MsgId:     1,
	})
	assert.Nil(t, err)
	t.Log(marker)
	t.Log(res)
}

func TestTableStore_CreateMsgTable(t *testing.T) {
	tableStore := getTestTableStore()
	defer tableStore.CloseGracefully()
	err := tableStore.CreateMsgTable("test-table", []ColumnInfo{
		{
			ColumnName: "create_time",
			ColumnType: ColumnTypeString,
			Nullable:   false,
		},
		{
			ColumnName: "sender",
			ColumnType: ColumnTypeInteger,
			Nullable:   false,
		}, {
			ColumnName: "receiver",
			ColumnType: ColumnTypeInteger,
			Nullable:   false,
		}, {
			ColumnName: "msgType",
			ColumnType: ColumnTypeInteger,
			Nullable:   false,
		}}, nil)
	assert.Nil(t, err)
}
