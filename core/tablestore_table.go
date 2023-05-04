package core

import (
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

// GetDBInfo 获取数据库信息
func (i *TableStore) GetDBInfo() (res DBInfo, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		res, err = i.GetDBInfoWithTx(txn)
		return err
	})
	if err != nil {
		return DBInfo{}, err
	}
	return
}

func (i *TableStore) GetDBInfoWithTx(txn *badger.Txn) (res DBInfo, err error) {
	key := GenerateDBKey()
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return DBInfo{}, nil
		} else {
			return DBInfo{}, err
		}
	}
	if err = item.Value(func(val []byte) error {
		if err := json.Unmarshal(val, &res); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return DBInfo{}, err
	}
	return
}

func (i *TableStore) SaveDBInfoWithTx(tx *badger.Txn, info DBInfo) error {
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	key := GenerateDBKey()
	err = tx.Set([]byte(key), infoBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetMsgTableInfo 获取表信息
func (i *TableStore) GetMsgTableInfo(tableName string) (res MsgTable, err error) {
	key := GenerateTableInfoKey(tableName)

	err = i.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			if err := json.Unmarshal(val, &res); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return MsgTable{}, errors.Errorf("TableStore|GetMsgTableInfo err:%s", err)
	}
	return
}

// CreateMsgTable 创建消息表
func (i *TableStore) CreateMsgTable(tableName string, columns []ColumnInfo, idxs []Index) error {
	if err := i.db.Update(func(txn *badger.Txn) error {
		// check if table already exist
		dbInfo, err := i.GetDBInfoWithTx(txn)
		if err != nil {
			return err
		}
		if exist := dbInfo.ExistTable(tableName); exist {
			return NewTableStoreErrWithExt(ErrTableAlreadyExist, map[string]interface{}{
				"table": tableName,
			})
		}

		// save table info
		id, err := i.generateNewTableId()
		if err != nil {
			return errors.Errorf("TableStore|CreateMsgTable i.generateNewTableId err:%s", err)
		}
		// TODO 要不要检查当前是否存在相应的表数据？

		// active index
		for _, idx := range idxs {
			idx.Enable = true
		}
		table := MsgTable{
			TableName: tableName,
			TableId:   id,
			Columns:   columns,
			Indexs:    idxs,
		}
		tableInfoKey := GenerateTableInfoKey(tableName)
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return errors.Errorf("TableStore|CreateMsgTable json.Marshal err:%s", err)
		}
		if err = txn.Set([]byte(tableInfoKey), tableBytes); err != nil {
			return errors.Errorf("TableStore|CreateMsgTable txn.Set err:%s", err)
		}

		// add table to meta info
		_ = dbInfo.AddTable(tableName)
		if err = i.SaveDBInfoWithTx(txn, dbInfo); err != nil {
			return errors.Errorf("TableStore|CreateMsgTable i.SaveDBInfoWithTx err:%s", err)
		}
		return nil
	}); err != nil {
		return errors.Errorf("TableStore|CreateMsgTable i.db.Update err:%s", err)
	}
	return nil
}

type GlobalTableId struct {
	Id uint64
}

// generateNewTableId 获取并增加表编码
func (i *TableStore) generateNewTableId() (tableId uint64, err error) {
	err = i.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(_globalTableIdKey))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			res := GlobalTableId{}

			err := json.Unmarshal(val, &res)
			if err != nil {
				return err
			}

			tableId = res.Id
			return nil
		})
		if err != nil {
			return err
		}

		incId := GlobalTableId{Id: tableId}
		incIdBytes, err := json.Marshal(incId)
		if err != nil {
			return err
		}
		err = txn.Set([]byte(_globalTableIdKey), incIdBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return
}

// DropMsgTable 卸载消息表
func (i *TableStore) DropMsgTable(tableName string) error {
	if err := i.db.Update(func(txn *badger.Txn) error {
		// del table data
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek([]byte(_msgKeyPrefix)); it.ValidForPrefix([]byte(_msgKeyFormat)); it.Next() {
			// TODO 验证迭代的过程中是否可以删除消息
			err := txn.Delete([]byte(_msgKeyFormat))
			if err != nil {
				return errors.Errorf("TableStore|DropMsgTable txn.Delete err:%s", err)
			}
		}

		// del table info
		tableInfoKey := GenerateTableInfoKey(tableName)
		if err := txn.Delete([]byte(tableInfoKey)); err != nil {
			return errors.Errorf("TableStore|DropMsgTable txn.Delete err:%s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// AddColumn 添加列
func (i *TableStore) AddColumn(tableName string, colInfo ColumnInfo) error {
	res, err := i.GetMsgTableInfo(tableName)
	if err != nil {
		return err
	}
	i.db.Update(func(txn *badger.Txn) error {

	})
}

// DropColumn 卸载列信息
func (i *TableStore) DropColumn() {

}

// RepairMsgTable 修复消息表
func (i *TableStore) RepairMsgTable() {

}

func (i *TableStore) GetIndexByFields() {

}
