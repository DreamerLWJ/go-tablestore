package sdk

import (
	"context"
	"go-tablestore/api"
)

type DB struct {
}

func (D *DB) CreateTable(ctx context.Context, req api.CreateTableReq) error {
	//TODO implement me
	panic("implement me")
}

func (D *DB) ListTable(ctx context.Context, req api.ListTableReq) (api.ListTableResp, error) {
	//TODO implement me
	panic("implement me")
}

func (D *DB) GetTable(ctx context.Context, req api.GetTableReq) (api.GetTableResp, error) {
	//TODO implement me
	panic("implement me")
}

func (D *DB) DropTable(ctx context.Context, req api.DropTableReq) error {
	//TODO implement me
	panic("implement me")
}

func (D *DB) ModifyTable(ctx context.Context, req api.ModifyTableReq) error {
	//TODO implement me
	panic("implement me")
}

func (D *DB) RepairTable(ctx context.Context, req api.RepairTableReq) error {
	//TODO implement me
	panic("implement me")
}
