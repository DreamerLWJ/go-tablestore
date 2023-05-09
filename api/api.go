package api

import "go-tablestore/core"

type Api interface {
	WriteData() error
}

type SearchOption struct {
}

type CreateTableReq struct {
	TableName string            `json:"tableName"` // 表名
	Columns   []core.ColumnInfo `json:"columns"`   // 列名
	Indexs    []core.Index      `json:"indexs"`    // 索引列表
}

type DropTableReq struct {
	TableName string `json:"tableName"`
}

type GetTableReq struct {
}

type GetTableResp struct {
}

type ListTableReq struct {
}

type ListTableResp struct {
}

type ModifyTableReq struct {
}

type RepairTableReq struct {
}
