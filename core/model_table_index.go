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

func GenerateIndexInfoPrefix(tableId uint64) string {
	return fmt.Sprintf(_indexInfoColKeyPrefix, tableId)
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

func GenerateIndexKeyPrefix(tableId uint64, idxId uint64) string {
	return fmt.Sprintf(_indexKeyPrefix, tableId, idxId)
}

// GenerateIndexKeyPrefixFromPlan 根据执行计划生成索引值
func GenerateIndexKeyPrefixFromPlan(tbInfo Table, plan ExecutionPlan, conds []QueryCond) string {
	if plan.PlanType != Idx {
		return ""
	}

	// columnName -> ColumnInfo
	colInfoMap := make(map[string]ColumnInfo, len(tbInfo.Columns))
	for _, column := range tbInfo.Columns {
		colInfoMap[column.ColumnName] = column
	}

	// columnName -> QueryCond
	condMap := make(map[string]QueryCond, len(conds))
	for _, cond := range conds {
		condMap[cond.ColumnName] = cond
	}

	matchKeyParts := make([]string, 0, len(plan.ColumnsCombine))
	for _, col := range plan.ColumnsCombine {
		var matchKeyPart string
		var err error

		cond, ok := condMap[col]
		if !ok {
			// TODO 查询异常问题
		}
		info, ok := colInfoMap[col]
		if !ok {
			// TODO 查询异常问题
		}
		matchKeyPart, err = GetPaddingValue(cond.ColumnValue, info.ColumnType)
		if err != nil {
			// TODO
		}

		matchKeyParts = append(matchKeyParts, matchKeyPart)
	}
	return fmt.Sprintf(_indexKeyFormat, tbInfo.TableId, plan.Idx.IndexId, strings.Join(matchKeyParts, _indexSeparate))
}
