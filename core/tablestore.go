package core

import (
	"github.com/dgraph-io/badger/v3"
	"go-tablestore/config"
)

type TableStore struct {
	db *badger.DB // TODO 索引和消息是否要存放在同一个 LSM Tree 中？
	op QueryOptimizer
}

func NewTableStore(dbCfg config.DBConfig) (*TableStore, error) {
	msgDir := "./msg"
	if dbCfg.DataDir != "" {
		msgDir = dbCfg.DataDir
	}

	msgDBOpt := badger.DefaultOptions(msgDir)

	msgDB, err := badger.Open(msgDBOpt)
	if err != nil {
		return nil, err
	}

	return &TableStore{db: msgDB, op: NewSimpleEqQueryOptimizer()}, nil
}

// CloseGracefully 请优雅关闭，不然会很头疼
func (i *TableStore) CloseGracefully() error {
	err := i.db.Flatten(0)
	if err != nil {
		// TODO log
		return err
	}
	err = i.db.Close()
	if err != nil {
		return err
	}
	return nil
}

// ClearAllData 清空所有数据
func (i *TableStore) ClearAllData() error {
	return i.db.DropAll()
}
