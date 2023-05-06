package core

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"strings"
	"testing"
)

const (
	_testTableId   = 233
	_testTableName = "test_table"

	_testMsgId = 666
)

func TestGenerateDBKey(t *testing.T) {
	key := GenerateDBInfoKey()
	assert.Equal(t, _dbKeyFormat, key)
}

func TestGenerateTableInfoKey(t *testing.T) {
	key := GenerateTableInfoKey(_testTableName)
	t.Log(key)
}

func TestGenerateMsgKey(t *testing.T) {
	key := GenerateMsgKey(_testTableId, _testMsgId)
	t.Log(key)
}

func TestGetPaddingMsgId(t *testing.T) {
	paddingMsgId := GetPaddingMsgId(_testMsgId)
	t.Log(paddingMsgId)
}

// cover cond
// -123(v1) > -200(v2)
// 0(v3) > -123(v1)
// 1 > -123
// 2 > 1
func TestGetPaddingValue(t *testing.T) {
	n1 := "-123"
	n2 := "123"
	n3 := "0"
	v1, err := GetPaddingValue(n1, ColumnTypeInteger)
	assert.Nil(t, err)

	v2, err := GetPaddingValue(n2, ColumnTypeInteger)
	assert.Nil(t, err)

	v3, err := GetPaddingValue(n3, ColumnTypeInteger)
	assert.Nil(t, err)

	orderStrs := []string{v3, v2, v1}
	sort.Strings(orderStrs)

	assert.Equal(t, v1, orderStrs[0])
	assert.Equal(t, v2, orderStrs[2])
	assert.Equal(t, v3, orderStrs[1])

	n4 := "-12.3"
	n5 := "12.3"
	n6 := "0.0"
	n7 := "-12.4"
	v4, err := GetPaddingValue(n4, ColumnTypeDouble)
	assert.Nil(t, err)

	v5, err := GetPaddingValue(n5, ColumnTypeDouble)
	assert.Nil(t, err)

	v6, err := GetPaddingValue(n6, ColumnTypeDouble)
	assert.Nil(t, err)

	v7, err := GetPaddingValue(n7, ColumnTypeDouble)
	assert.Nil(t, err)

	t.Log(v4 > v5)
	t.Log(v4 > v7)
	t.Log(v4 > v6)
	t.Log(v5 > v6)
}

func TestName(t *testing.T) {
	t.Log(strings.Compare("-", "+"))
}
