package core

type ColumnType int

const (
	ColumnTypeString  ColumnType = iota + 1 // 字符串类型
	ColumnTypeInteger                       // 整数类型
	ColumnTypeDouble                        // 浮点类型
)

// MsgTable 消息表，表没有主键，主键就是消息键
type MsgTable struct {
	TableName string       `json:"tableName"` // 表名
	Columns   []ColumnInfo `json:"columns"`   // 列名
	Indexs    []Index      `json:"indexs"`    // 索引列表
}

// Index 索引设计，目前仅支持 B+ 树二级索引
type Index struct {
	// TODO 索引设计，不需要排序，理由是索引顺序也有意义
	// TODO 需要排序，理由是更好判断查询是否匹配索引
	Fields []string `json:"fields"` // 索引字段
	Enable bool     // 索引是否可用
}

// ColumnInfo 数据列信息
type ColumnInfo struct {
	ColumnName string     `json:"columnName"` // 列名
	ColumnType ColumnType `json:"columnType"` // 列类型
	Nullable   bool       `json:"nullable"`   // 是否可空
}
