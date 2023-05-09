package core

import "strings"

// QueryOptimizer 查询优化器
// TODO 带有索引下推的优化器？
type QueryOptimizer interface {
	GeneratePlan(colNames []string, tbInfo TableInfo) ExecutionPlan
}

type SimpleEqQueryOptimizer struct {
}

func NewSimpleEqQueryOptimizer() SimpleEqQueryOptimizer {
	return SimpleEqQueryOptimizer{}
}

// GeneratePlan 初期的优化器，仅支持等值查询判断；对于范围查询可以先定位到
func (s SimpleEqQueryOptimizer) GeneratePlan(colNames []string, tbInfo TableInfo) (res ExecutionPlan) {
	var maxHitIdx Index        // 命中最长的索引
	maxHitIdxPrefixLength := 0 // 命中最长的索引的前缀长度
	var maxHitCombine []string // 命中最长的列组合

	// to lower
	for i, col := range colNames {
		colNames[i] = strings.ToLower(col)
	}

	existMsgId := false
	combines := permute(colNames)
	for _, index := range tbInfo.Indexs {
		for _, combine := range combines {
			hitIdxLength := 0
			for i := 0; i < len(index.ColumnNames) && i < len(combine); i++ {
				if index.ColumnNames[i] == combine[i] {
					hitIdxLength++

					if combine[i] == "msgid" {
						existMsgId = true
					}
				}
			}

			if hitIdxLength > maxHitIdxPrefixLength {
				maxHitIdxPrefixLength = hitIdxLength
				maxHitIdx = index
				maxHitCombine = combine
			}
		}
	}
	// gather
	if maxHitIdxPrefixLength != 0 {
		res.PlanType = Idx
		res.Idx = maxHitIdx
		res.ColumnsCombine = maxHitCombine
		res.IdxPrefixLength = maxHitIdxPrefixLength
	} else {
		if existMsgId {
			res.PlanType = Primary
		} else {
			res.PlanType = All
		}
	}
	return
}

type PlanType int

const (
	All     PlanType = iota + 1 // 全搜索
	Idx                         // 索引搜索
	Primary                     // 主键搜索
)

// ExecutionPlan 执行计划
type ExecutionPlan struct {
	PlanType        PlanType
	Idx             Index
	IdxPrefixLength int
	ColumnsCombine  []string
}

func permute(strs []string) [][]string {
	var res [][]string
	used := make([]bool, len(strs))
	backtrack([]string{}, strs, &res, used)
	return res
}

func backtrack(cur []string, strs []string, res *[][]string, used []bool) {
	if len(cur) == len(strs) {
		temp := make([]string, len(cur))
		copy(temp, cur)
		*res = append(*res, temp)
		return
	}
	for i := 0; i < len(strs); i++ {
		if used[i] {
			continue
		}
		used[i] = true
		cur = append(cur, strs[i])
		backtrack(cur, strs, res, used)
		used[i] = false
		cur = cur[:len(cur)-1]
	}
}
