package core

// Message 消息存储的格式
type Message struct {
	MsgID        uint64            `json:"msgID"`
	ColumnValues map[string]string `json:"columns"` // 消息传递统一使用 String
}
