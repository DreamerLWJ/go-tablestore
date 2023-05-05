package core

import (
	"fmt"
	"sort"
	"strings"
)

// Index 索引设计，目前仅支持 B+ 树二级索引
type Index struct {
	IndexId uint64 `json:"indexCode"` // 索引唯一标识
	// TODO 索引设计，不需要排序，理由是索引顺序也有意义
	// TODO 需要排序，理由是更好判断查询是否匹配索引
	ColumnNames []string `json:"fields"` // 索引字段，统一转为小写字母
	Enable      bool     `json:"enable"` // 索引是否可用
}

// GenerateIndexInfoIdKey 生成索引信息的 id 索引
func GenerateIndexInfoIdKey(tableId uint64, idxId uint64) string {
	return fmt.Sprintf(_indexInfoIdKeyFormat, tableId, idxId)
}

// GenerateIndexInfoColumnsKey 生成索引信息的列索引
func GenerateIndexInfoColumnsKey(tableId uint64, colNames []string) string {
	sort.Strings(colNames)
	colKeySuffix := strings.Join(colNames, _indexSeparate)
	return fmt.Sprintf(_indexInfoColKeyFormat, tableId, colKeySuffix)
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
