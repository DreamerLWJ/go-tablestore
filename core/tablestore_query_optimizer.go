package core

// QueryOptimizer 查询优化器
type QueryOptimizer interface {
	GeneratePlan(FilterCond map[string]string, availableIdx []Index)
}

type SimpleEqQueryOptimizer struct {
}

func (s SimpleEqQueryOptimizer) GeneratePlan(FilterCond map[string]string, availableIdx []Index) {
	// gather
}

// ExecutionPlan 执行计划
type ExecutionPlan struct {
}
