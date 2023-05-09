package api

import "context"

type DB interface {
	CreateTable(ctx context.Context, req CreateTableReq) error

	ListTable(ctx context.Context, req ListTableReq) (ListTableResp, error)

	GetTable(ctx context.Context, req GetTableReq) (GetTableResp, error)

	DropTable(ctx context.Context, req DropTableReq) error

	ModifyTable(ctx context.Context, req ModifyTableReq) error

	RepairTable(ctx context.Context, req RepairTableReq) error
}
