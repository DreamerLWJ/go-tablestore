package core

type Message struct {
	MsgID   uint64            `json:"msgID"`
	Columns map[string]string `json:"columns"` // 消息传递统一使用 String
}
