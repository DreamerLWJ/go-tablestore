package core

type Message struct {
	MsgID   uint64                 `json:"msgID"`
	Columns map[string]interface{} `json:"columns"`
}

// IndexSetting 索引设计
type IndexSetting struct {
	// TODO 索引设计，不需要排序，理由是索引顺序也有意义
	// TODO 需要排序，理由是更好判断查询是否匹配索引
	Fields []string `json:"fields"`
	Enable bool     // 索引是否可用
}

// QueryOption 查询条件
type QueryOption struct {
	EqOpt map[string]interface{}
}
