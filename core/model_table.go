package core

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"math"
	"strconv"
	"strings"
)

type ColumnType int

const (
	ColumnTypeString  ColumnType = iota + 1 // 字符串类型
	ColumnTypeInteger                       // 整数类型
	ColumnTypeDouble                        // 浮点类型
)

const (
	_msgKeyPrefix          = "msg|t:%d|"
	_indexKeyPrefix        = "idx_val|t:%d|i:%d|"
	_indexInfoIdKeyPrefix  = "idx_inf_id|t:%d|"
	_indexInfoColKeyPrefix = "idx_inf_col|t:%d|"

	// 数据库元信息
	_dbKeyFormat = "db_info"

	// 表信息：tb_inf|t:[table_name]
	_tableInfoKeyFormat = "tb_inf|t:%s"

	// 二级索引信息的索引键：idx_inf_id|t:%d|tid:%d，索引唯一id
	_indexInfoIdKeyFormat = _indexInfoIdKeyPrefix + "tid:%d" // 索引配置

	// 二级索引信息：idx_inf_col|t:%d|col:%s，索引排序后的列
	_indexInfoColKeyFormat = _indexInfoColKeyPrefix + "col:%s" //

	// 表消息主键设计：msg|t:[table_code]|[padding_msg_id]
	_msgKeyFormat = _msgKeyPrefix + "%s" // 消息主键

	// 表二级索引设计：idx_val|t:[table_code]|i:[idx_code]|v:[idx_cols]
	_indexKeyFormat = _indexKeyPrefix + "v:%s" // 二级索引，

	// 全局 TableId
	_globalTableIdKey = "glb_tid"

	_nullField = "NULL"

	_indexSeparate = "-"
)

// GlobalDBInfo 目前仅支持全局一个 DB
type GlobalDBInfo struct {
	TableNames []string // 拥有的消息表
}

func (d *GlobalDBInfo) ExistTable(tableName string) (exist bool) {
	for _, name := range d.TableNames {
		if name == tableName {
			exist = true
			return
		}
	}
	return
}

func (d *GlobalDBInfo) AddTable(tableName string) bool {
	if exist := d.ExistTable(tableName); exist {
		return false
	}

	d.TableNames = append(d.TableNames, tableName)
	return true
}

// Table 消息表，表没有主键，主键就是消息键
type Table struct {
	TableName string       `json:"tableName"` // 表名
	TableId   uint64       `json:"tableCode"` // 表的唯一标识，用于节省存储空间
	Columns   []ColumnInfo `json:"columns"`   // 列名
	Indexs    []Index      `json:"indexs"`    // 索引列表
}

// ExistColumn return if column exist
func (m *Table) ExistColumn(colName string) (exist bool) {
	for _, column := range m.Columns {
		if column.ColumnName == colName {
			return true
		}
	}
	return false
}

// AddColumnInfo 添加列信息
func (m *Table) AddColumnInfo(colInfo ColumnInfo) bool {
	if m.ExistColumn(colInfo.ColumnName) {
		return false
	}
	return true
}

// DelColumnInfo 删除列信息
func (m *Table) DelColumnInfo(colName string) bool {
	index := -1
	for i, column := range m.Columns {
		if column.ColumnName == colName {
			index = i
		}
	}

	if index == -1 {
		return false
	}
	m.Columns = append(m.Columns[:index], m.Columns[index+1:]...)
	return true
}

// ExistIndexInfo 返回是否存在对应的索引
func (m *Table) ExistIndexInfo(colNames []string) (exist bool) {
	colMerge := strings.Join(colNames, _indexSeparate)
	colMerge = strings.ToLower(colMerge)
	for _, index := range m.Indexs {
		idxMerge := strings.Join(index.ColumnNames, _indexSeparate)
		if colMerge == idxMerge {
			return true
		}
	}
	return false
}

// AddIndexInfo 添加索引
func (m *Table) AddIndexInfo(idx Index) bool {
	if exist := m.ExistIndexInfo(idx.ColumnNames); exist {
		return false
	}

	m.Indexs = append(m.Indexs, idx)
	return true
}

// ColumnInfo 数据列信息
type ColumnInfo struct {
	ColumnName string     `json:"columnName"` // 列名
	ColumnType ColumnType `json:"columnType"` // 列类型
	Nullable   bool       `json:"nullable"`   // 是否可空
}

// GetPaddingValue 可以选择在此扩展更多类型以达到节省空间的目的
func GetPaddingValue(val string, colType ColumnType) (string, error) {
	switch colType {
	case ColumnTypeInteger:
		valI, err := cast.ToInt64E(val)
		if err != nil {
			return "", NewTableStoreErrWithExt(ErrMessageColumnTypeInvalid, map[string]interface{}{
				"val":         valI,
				"target_type": "integer",
			})
		}

		var val string
		if valI < 0 {
			val = "0" + fmt.Sprintf("%020d", uint64(valI))
		} else {
			val = "1" + fmt.Sprintf("%020d", uint64(valI))
		}
		return val, nil
	case ColumnTypeDouble:
		valD, err := cast.ToFloat64E(val)
		if err != nil {
			return "", NewTableStoreErrWithExt(ErrMessageColumnTypeInvalid, map[string]interface{}{
				"val":         valD,
				"target_type": "double",
			})
		}

		maxFloat := math.MaxFloat64 / 2
		return strconv.FormatFloat(valD+maxFloat, 'f', -1, 64), nil
	case ColumnTypeString:
		return val, nil
	default:
		return "", errors.Errorf("unsupport type:%d", colType)
	}
}

func GenerateDBInfoKey() string {
	return _dbKeyFormat
}

func GenerateTableInfoKey(tableName string) string {
	return fmt.Sprintf(_tableInfoKeyFormat, tableName)
}

// GenerateMsgKey 生成基于消息 ID 的主键
func GenerateMsgKey(tableId uint64, msgId uint64) string {
	return fmt.Sprintf(_msgKeyFormat, tableId, GetPaddingMsgId(msgId))
}

func GetPaddingMsgId(msgId uint64) string {
	//keyBytes := make([]byte, 8)
	//binary.BigEndian.PutUint64(keyBytes, msgId)
	return fmt.Sprintf("%020d", msgId)
}
