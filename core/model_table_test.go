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

	orderStrs := []string{v1, v2, v3}
	sort.Strings(orderStrs)

	for _, str := range orderStrs {
		switch str {
		case v1:
			t.Log(n1)
		case v2:
			t.Log(n2)
		case v3:
			t.Log(n3)
		}
	}
}

func TestName(t *testing.T) {
	t.Log(strings.Compare("-", "+"))
}
