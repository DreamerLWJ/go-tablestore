package core

import (
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
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
	key := GenerateDBInfoKey()
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
	key := GenerateDBInfoKey()
	err = tx.Set([]byte(key), infoBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetMsgTableInfo 获取表信息
func (i *TableStore) GetMsgTableInfo(tableName string) (res MsgTable, exist bool, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		res, exist, err = i.GetMsgTableInfoWithTx(txn, tableName)
		return err
	})
	return
}

func (i *TableStore) GetMsgTableInfoWithTx(txn *badger.Txn, tableName string) (res MsgTable, exist bool, err error) {
	key := GenerateTableInfoKey(tableName)
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			exist = false
			return MsgTable{}, false, nil
		}
		return MsgTable{}, false, err
	}
	err = item.Value(func(val []byte) error {
		if err := json.Unmarshal(val, &res); err != nil {
			return err
		}
		exist = true
		return nil
	})
	if err != nil {
		return MsgTable{}, false, err
	}
	return
}

// SaveMsgTableInfo 保存表信息
func (i *TableStore) SaveMsgTableInfo(tbInfo MsgTable) error {
	if err := i.db.Update(func(txn *badger.Txn) error {
		if err := i.SaveMsgTableInfoWithTx(txn, tbInfo); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (i *TableStore) SaveMsgTableInfoWithTx(txn *badger.Txn, tbInfo MsgTable) error {
	key := GenerateTableInfoKey(tbInfo.TableName)
	tbInfoBytes, err := json.Marshal(tbInfo)
	if err != nil {
		return err
	}
	if err := txn.Set([]byte(key), tbInfoBytes); err != nil {
		return err
	}
	return nil
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
			if errors.Is(err, badger.ErrKeyNotFound) {
				tableId = 1
			} else {
				return errors.Errorf("TableStore|generateNewTableId txn.Get err:%s", err)
			}
		} else {
			if err = item.Value(func(val []byte) error {
				res := GlobalTableId{}

				err := json.Unmarshal(val, &res)
				if err != nil {
					return err
				}

				tableId = res.Id
				return nil
			}); err != nil {
				return errors.Errorf("TableStore|generateNewTableId err:%s", err)
			}
		}

		incId := GlobalTableId{Id: tableId + 1}
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
	if err := i.db.Update(func(txn *badger.Txn) error {
		tableInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|AddColumn i.GetMsgTableInfoWithTx err:%s", err)
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		if exist := tableInfo.ExistColumn(colInfo.ColumnName); exist {
			return NewTableStoreErrWithExt(ErrTableAlreadyExist, map[string]interface{}{
				"column": colInfo.ColumnName,
			})
		}

		tableInfo.AddColumnInfo(colInfo)
		if err = i.SaveMsgTableInfo(tableInfo); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// DropColumn 删除列，同时会删除掉列的所有数据
func (i *TableStore) DropColumn(tableName string, columnName string) error {
	if err := i.db.Update(func(txn *badger.Txn) error {
		tbInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|DropColumn i.GetMsgTableInfoWithTx err:%s", err)
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		tbInfo.DelColumnInfo(columnName)
		err = i.SaveMsgTableInfoWithTx(txn, tbInfo)
		if err != nil {
			return errors.Errorf("TableStore|DropColumn i.SaveMsgTableInfoWithTx err:%s", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// RepairMsgTable 修复消息表
func (i *TableStore) RepairMsgTable(tableName string) error {
	// TODO
	return nil
}

// CreateIndex 为某个表创建索引
// TODO 思考：创建索引时需要协程异步，还是上 MDL 锁
func (i *TableStore) CreateIndex(tableName string, idx Index) error {
	// 检查索引是否已经存在
	_, exist, err := i.GetIndexInfoByColumns(tableName, idx.ColumnNames)
	if err != nil {
		return errors.Errorf("TableStore|CreateIndex i.GetIndexInfoById err:%s", err)
	}
	if exist {
		return errors.Errorf("TableStore|CreateIndex index %v already exist", idx.ColumnNames)
	}

	// 获取表信息
	tbInfo, exist, err := i.GetMsgTableInfo(tableName)
	if err != nil {
		return errors.Errorf("TableStore|CreateIndex i.GetMsgTableInf err:%s", err)
	}
	if !exist {
		return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
			"table": tableName,
		})
	}
	colInfoMap := make(map[string]ColumnInfo, len(tbInfo.Columns))
	for _, column := range tbInfo.Columns {
		colInfoMap[column.ColumnName] = column
	}

	err = i.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// 给所有值加上索引
		for it.Seek([]byte(_msgKeyPrefix)); it.ValidForPrefix([]byte(_msgKeyPrefix)); it.Next() {
			valBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			msg := Message{}
			err = json.Unmarshal(valBytes, &msg)
			if err != nil {
				return err
			}

			idxKey := GenerateIndexKey(tbInfo.TableId, colInfoMap, idx, &msg)
			// 索引值指向主键
			msgIdStr := cast.ToString(msg.MsgID)

			// 新增索引值
			err = txn.Set([]byte(idxKey), []byte(msgIdStr))
			if err != nil {
				return err
			}
		}

		// 插入索引配置
		idxInfoKey := GenerateIndexInfoIdKey(tbInfo.TableId, idx.IndexId)
		idxVBytes, err := json.Marshal(idx)
		if err != nil {
			return err
		}
		err = txn.Set([]byte(idxInfoKey), idxVBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("TableStore|CreateIndex db.Update err:%s", err)
	}
	return nil
}

// ListIndex 列出所有的索引
func (i *TableStore) ListIndex() (res []Index, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek([]byte(_indexInfoIdKeyPrefix)); it.ValidForPrefix([]byte(_indexInfoIdKeyPrefix)); it.Next() {
			item := it.Item()
			vBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			temp := Index{}
			err = json.Unmarshal(vBytes, &temp)
			if err != nil {
				return err
			}
			res = append(res, temp)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}

// GetIndexInfoByColumns 根据列获取索引信息
func (i *TableStore) GetIndexInfoByColumns(tableName string, cols []string) (res Index, exist bool, err error) {
	if err = i.db.View(func(txn *badger.Txn) error {
		tbInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|GetIndexInfoByColumns")
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		idxKey := GenerateIndexInfoColumnsKey(tbInfo.TableId, cols)
		item, err := txn.Get([]byte(idxKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				exist = false
				return nil
			}
			return errors.Errorf("TableStore|GetIndexInfoByColumns txn.Get err:%s", err)
		}

		err = item.Value(func(val []byte) error {
			if err := json.Unmarshal(val, &res); err != nil {
				return errors.Errorf("TableStore|GetIndexInfoByColumns json.Unmarshal err:%s", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Index{}, false, err
	}
	return
}

// GetIndexInfoById 根据索引 id 获取索引信息
func (i *TableStore) GetIndexInfoById(tableName string, idxId uint64) (res Index, exist bool, err error) {
	var vBytes []byte
	if err = i.db.View(func(txn *badger.Txn) error {
		tbInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|GetIndexInfoById i.GetMsgTableInfoWithTx err:%s", err)
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		idxInfoKey := GenerateIndexInfoIdKey(tbInfo.TableId, idxId)
		item, err := txn.Get([]byte(idxInfoKey))
		if err != nil {
			return err
		}
		vBytes, err = item.ValueCopy(vBytes)
		if err != nil {
			return err
		}
		err = json.Unmarshal(vBytes, &res)
		if err != nil {
			return errors.Errorf("TableStore|GetIndexInfoById json.Unmarshal err:%s", err)
		}
		return nil
	}); err != nil {
		return Index{}, false, err
	}
	return
}

// GetAvailableIndex 获取可选择的索引列表
func (i *TableStore) GetAvailableIndex() (res []Index, err error) {
	tx := i.db.NewTransaction(false)
	defer func() {
		if err != nil {
			tx.Discard()
		} else {
			err := tx.Commit()
			if err != nil {
				// TODO log
			}
		}
	}()
	res, err = i.GetAvailableIndexWithTx(tx)
	return
}

// GetAvailableIndexWithTx 用于 MVCC 级别
func (i *TableStore) GetAvailableIndexWithTx(txn *badger.Txn) ([]Index, error) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var result []Index
	for it.Seek([]byte(_indexInfoIdKeyPrefix)); it.ValidForPrefix([]byte(_indexInfoIdKeyPrefix)); it.Next() {
		item := it.Item()
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, errors.Errorf("TableStore|GetAvailableIndexWithTx item.ValueCopy err:%s", err)
		}

		temp := Index{}

		err = json.Unmarshal(valueBytes, &temp)
		if err != nil {
			return nil, errors.Errorf("TableStore|GetAvailableIndexWithTx json.Unmarshal err:%s", err)
		}

		if temp.Enable {
			result = append(result, temp)
		}
	}
	return result, nil
}
