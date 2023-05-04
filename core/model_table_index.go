package core

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"strings"
)

// Index 索引设计，目前仅支持 B+ 树二级索引
type Index struct {
	IndexId uint64 `json:"indexCode"` // 索引唯一标识
	// TODO 索引设计，不需要排序，理由是索引顺序也有意义
	// TODO 需要排序，理由是更好判断查询是否匹配索引
	ColumnNames []string `json:"fields"` // 索引字段，统一转为小写字母
	Enable      bool     `json:"enable"` // 索引是否可用
}

// GenerateIndexInfoKey 生成索引信息的索引
func GenerateIndexInfoKey(tableId uint64, idxId uint64) string {
	return fmt.Sprintf(_indexInfoKeyFormat, tableId, idxId)
}

// GenerateIndexKey 生成二级索引 key
func GenerateIndexKey(tableId uint64, colInfos map[string]ColumnInfo, idx Index, msg *Message) string {
	// TODO 优化字符串拼接
	keyParts := make([]string, 0, len(idx.ColumnNames))
	for _, column := range idx.ColumnNames {
		var keyPart string
		var err error
		colInfo, ok := colInfos[column]
		if !ok {
			// TODO invalid idx config
		}

		colData, ok := msg.ColumnValues[colInfo.ColumnName]
		if !ok {
			colData = _nullField
		}
		if column == "msgid" {
			keyPart = GetPaddingMsgId(msg.MsgID)
		} else {
			keyPart, err = GetPaddingValue(colData, colInfo.ColumnType)
			if err != nil {
				// TODO
			}
		}
		keyParts = append(keyParts, keyPart)
	}
	return fmt.Sprintf(_indexKeyFormat, tableId, idx.IndexId, strings.Join(keyParts, _indexSeparate))
}

// CreateIndex 创建索引
// TODO 思考：创建索引时需要协程异步，还是上 MDL 锁
func (i *TableStore) CreateIndex(tableName string, idx Index) error {
	// 检查索引是否已经存在
	_, exist, err := i.GetIndexInfo(idx.ColumnNames, idx.IndexId)
	if err != nil {
		return errors.Errorf("TableStore|CreateIndex i.GetIndexInfo err:%s", err)
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
		idxInfoKey := GenerateIndexInfoKey(tbInfo.TableId, idx.IndexId)
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

		for it.Seek([]byte(_indexInfoKeyPrefix)); it.ValidForPrefix([]byte(_indexInfoKeyPrefix)); it.Next() {
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

// GetIndexInfo 获取索引
func (i *TableStore) GetIndexInfo(tableName string, idxId uint64) (res Index, exist bool, err error) {
	var vBytes []byte
	if err = i.db.View(func(txn *badger.Txn) error {
		tbInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|GetIndexInfo i.GetMsgTableInfoWithTx err:%s", err)
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		idxInfoKey := GenerateIndexInfoKey(tbInfo.TableId, idxId)
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
			return errors.Errorf("TableStore|GetIndexInfo json.Unmarshal err:%s", err)
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
	for it.Seek([]byte(_indexInfoKeyPrefix)); it.ValidForPrefix([]byte(_indexInfoKeyPrefix)); it.Next() {
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
