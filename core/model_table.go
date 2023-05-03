package core

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
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
	_msgKeyPrefix       = "msg|t:%d|"
	_indexKeyPrefix     = "idx_val|t:%d|"
	_indexInfoKeyPrefix = "idx_cfg|t:%d|"

	// 数据库元信息
	_dbKeyFormat = "db_info"

	// 表信息：tb_inf|t:[table_name]
	_tableInfoKeyFormat = "tb_inf|t:%s"

	// 二级索引信息：idx_inf|t:%d|%d
	_indexInfoKeyFormat = _indexInfoKeyPrefix + "%d" // 索引配置

	// 表消息主键设计：msg|t:[table_code]|[padding_msg_id]
	_msgKeyFormat = _msgKeyPrefix + "%s" // 消息主键

	// 表二级索引设计：idx_val|t:[table_code]|i:[idx_code]|v:[idx_cols]
	_indexKeyFormat = _indexKeyPrefix + "i:%d|%s" // 二级索引，

	// 全局 TableId
	_globalTableIdKey = "glb_tid"

	_nullField = "NULL"

	_indexSeparate = "-"
)

type DBInfo struct {
	TableNames []string // 拥有的消息表
}

func (d *DBInfo) ExistTable(tableName string) (exist bool) {
	for _, name := range d.TableNames {
		if name == tableName {
			exist = true
			return
		}
	}
	return
}

func (d *DBInfo) AddTable(tableName string) bool {
	if exist := d.ExistTable(tableName); exist {
		return false
	}

	d.TableNames = append(d.TableNames, tableName)
	return true
}

// MsgTable 消息表，表没有主键，主键就是消息键
type MsgTable struct {
	TableName string       `json:"tableName"` // 表名
	TableId   uint64       `json:"tableCode"` // 表的唯一标识，用于节省存储空间
	Columns   []ColumnInfo `json:"columns"`   // 列名
	Indexs    []Index      `json:"indexs"`    // 索引列表
}

// ExistColumn return if column exist
func (m *MsgTable) ExistColumn(colName string) (exist bool) {
	for _, column := range m.Columns {
		if column.ColumnName == colName {
			return true
		}
	}
	return false
}

// ExistIndex return if index exist
func (m *MsgTable) ExistIndex(colNames []string) (exist bool) {

}

// Index 索引设计，目前仅支持 B+ 树二级索引
type Index struct {
	IndexId uint64 `json:"indexCode"` // 索引唯一标识
	// TODO 索引设计，不需要排序，理由是索引顺序也有意义
	// TODO 需要排序，理由是更好判断查询是否匹配索引
	ColumnNames []string `json:"fields"` // 索引字段，统一转为小写字母
	Enable      bool     `json:"enable"` // 索引是否可用
}

// ColumnInfo 数据列信息
type ColumnInfo struct {
	ColumnName string     `json:"columnName"` // 列名
	ColumnType ColumnType `json:"columnType"` // 列类型
	Nullable   bool       `json:"nullable"`   // 是否可空
}

// GenerateIndexKey 生成二级索引 key
func GenerateIndexKey(tableId uint64, colInfos map[string]ColumnInfo, idx Index, msg *Message) string {
	// TODO 优化字符串拼接
	keyParts := make([]string, 0, len(idx.ColumnNames))
	for _, column := range idx.ColumnNames {
		var keyPart string
		var err error
		colInfo, ok := colInfos[column]
		if !ok {
			// TODO invalid idx config
		}

		colData, ok := msg.ColumnValues[colInfo.ColumnName]
		if !ok {
			colData = _nullField
		}
		if column == "msgid" {
			keyPart = GetPaddingMsgId(msg.MsgID)
		} else {
			keyPart, err = GetPaddingValue(colData, colInfo.ColumnType)
			if err != nil {
				// TODO
			}
		}
		keyParts = append(keyParts, keyPart)
	}
	return fmt.Sprintf(_indexKeyFormat, tableId, idx.IndexId, strings.Join(keyParts, _indexSeparate))
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

		s := strconv.FormatInt(valI, 10)
		return fmt.Sprintf("%020s", s), nil
	case ColumnTypeDouble:
		valD, err := cast.ToFloat64E(val)
		if err != nil {
			return "", NewTableStoreErrWithExt(ErrMessageColumnTypeInvalid, map[string]interface{}{
				"val":         valD,
				"target_type": "double",
			})
		}

		s := strconv.FormatFloat(valD, 'f', -1, 64)
		// 补零操作
		for len(s) < 20 {
			s = "0" + s
		}
		return s, nil
	case ColumnTypeString:
		return val, nil
	default:
		return "", errors.Errorf("unsupport type:%d", colType)
	}
}

func GenerateIndexInfoKey(tableId uint64, idxId uint64) string {
	return fmt.Sprintf(_indexInfoKeyFormat, tableId, idxId)
}

func GenerateDBKey() string {
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
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, msgId)
	return fmt.Sprintf("%020d", keyBytes)
}
