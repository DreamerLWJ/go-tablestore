package core

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

// SaveMessage 增加消息
func (i *TableStore) SaveMessage(tableName string, msg *Message) error {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Errorf("TableStore|SaveMessage json.Marshal err:%s", err)
	}

	err = i.db.Update(func(txn *badger.Txn) error {
		tbInfo, exist, err := i.GetMsgTableInfoWithTx(txn, tableName)
		if err != nil {
			return errors.Errorf("TableStore|SaveMessage i.GetMsgTableInfo err:%s", err)
		}
		if !exist {
			return NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
				"table": tableName,
			})
		}

		// TODO check if msgId exist
		msgKey := GenerateMsgKey(tbInfo.TableId, msg.MsgID)

		// 首先保存主键
		err = txn.Set([]byte(msgKey), msgBytes)
		if err != nil {
			return errors.Errorf("TableStore|SaveMessage txn.Set err:%s", err)
		}

		// 生成索引
		indexes, err := i.GetAvailableIndexWithTx(txn)
		if err != nil {
			return err
		}
		// to map
		colInfoMap := make(map[string]ColumnInfo, len(tbInfo.Columns))
		for _, column := range tbInfo.Columns {
			colInfoMap[column.ColumnName] = column
		}

		msgIDStr := cast.ToString(msg.MsgID)
		for _, index := range indexes {
			indexKey := GenerateIndexKey(tbInfo.TableId, colInfoMap, index, msg)
			if err := txn.Set([]byte(indexKey), []byte(msgIDStr)); err != nil {
				return errors.Errorf("TableStore|SaveMessage txn.Set err:%s", err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("TableStore|SaveMessage db.Update err:%s", err)
	}
	return nil
}

// saveMessageIndex 保存消息的二级索引
func (i *TableStore) saveMessageIndex() {

}

type MessageQuery struct {
	TableName  string            `json:"tableName"` // 表名
	MsgId      uint64            `json:"msgId"`     // 按照 msgId 查询
	Marker     uint64            `json:"marker"`
	PageSize   int               `json:"pageSize"`
	FilterCond map[string]string `json:"filterCond"` // 过滤列条件，注意与索引结合
}

func (i *TableStore) QueryMessage(query MessageQuery) (res []Message, nextMarker uint64, err error) {
	// 检查表是否存在
	tbInfo, exist, err := i.GetMsgTableInfo(query.TableName)
	if err != nil {
		return nil, 0, errors.Errorf("TableStore|QueryMessage i.GetMsgTableInfo err:%s", err)
	}
	if !exist {
		return nil, 0, NewTableStoreErrWithExt(ErrTableNotFound, map[string]interface{}{
			"table": query.TableName,
		})
	}

	var seekKey string
	var seekKeyPrefix string
	// 优先采纳 msgId 的查询
	if query.MsgId != 0 {
		seekKey = GenerateMsgKey(tbInfo.TableId, query.MsgId)
		seekKeyPrefix = fmt.Sprintf(_msgKeyPrefix, tbInfo.TableId)
		return
	} else {
		// 优化器选择索引
		// TODO
	}

	result := make([]Message, 0, query.PageSize)
	var nextMsgID uint64

	err = i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(seekKey)); it.ValidForPrefix([]byte(seekKeyPrefix)); it.Next() {
			item := it.Item()

			valueBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			msgItem := Message{}
			err = json.Unmarshal(valueBytes, &msgItem)
			if err != nil {
				return err
			}

			// TODO 检查是否符合目标条件

			result = append(result, msgItem)
			if len(result) >= query.PageSize {
				nextMarker = msgItem.MsgID
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, errors.Errorf("TableStore|QueryMessage err:%s", err)
	}
	return result, nextMsgID, nil
}

// PrintAll 获取所有的键值
// TODO 删掉
func (i *TableStore) PrintAll(prefix string) error {
	err := i.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			vBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			fmt.Printf("key:%s, val:%s\n", string(item.Key()), string(vBytes))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
