package core

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"go-tablestore/config"
	"strings"
)

type TableStore struct {
	db *badger.DB // TODO 索引和消息是否要存放在同一个 LSM Tree 中？
}

func NewBadgerDb(dbCfg config.DBConfig) (*TableStore, error) {
	msgDir := "./msg"
	if dbCfg.DataDir != "" {
		msgDir = dbCfg.DataDir
	}

	msgDBOpt := badger.DefaultOptions(msgDir)

	msgDB, err := badger.Open(msgDBOpt)
	if err != nil {
		return nil, err
	}

	return &TableStore{db: msgDB}, nil
}

func generateIdxKey(idxSetting Index, msg Message) string {
	idxName := strings.Join(idxSetting.ColumnNames, _indexSeparate)
	idxValRaw := make([]string, 0, len(idxSetting.ColumnNames)+1)

	needAppendMsgId := true
	for _, field := range idxSetting.ColumnNames {
		if strings.ToLower(field) == "msgid" {
			idxValRaw = append(idxValRaw, cast.ToString(msg.MsgID))
			needAppendMsgId = false
		} else {
			fieldVal, ok := msg.ColumnValues[field]
			if !ok {
				idxValRaw = append(idxValRaw, _nullField)
			} else {
				idxValRaw = append(idxValRaw, cast.ToString(fieldVal))
			}
		}
	}

	if needAppendMsgId {
		idxValRaw = append(idxValRaw, cast.ToString(msg.MsgID))
	}

	return getIdxKeyWithNameVal(idxName, strings.Join(idxValRaw, _indexSeparate))
}

func getIdxKeyWithNameVal(idxName string, idxVal string) string {
	return fmt.Sprintf(_indexKeyFormat, idxName, idxVal)
}

// GetDataByMsgId 根据 MsgId 获取
func (i *TableStore) GetDataByMsgId(tableName string, msgID uint64) (Message, error) {
	tableInfo, err := i.GetMsgTableInfo(tableName)
	if err != nil {
		return Message{}, err
	}

	key := GenerateMsgKey(tableName, msgID)

	var result []byte
	err := i.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valueCopy, err := item.ValueCopy(result)
		if err != nil {
			return err
		}
		result = valueCopy
		return nil
	})

	if err != nil {
		return Message{}, errors.Errorf("TableStore|GetDataByMsgId db.View err:%s", err)
	}

	msgRes := Message{}
	err = json.Unmarshal(result, &msgRes)
	if err != nil {
		return Message{}, errors.Errorf("TableStore|json.Unmarshal err:%s", err)
	}
	return Message{}, nil
}

// SaveMessage 增加消息
func (i *TableStore) SaveMessage(msg Message) error {
	key := getMsgIDKey(msg.MsgID)
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Errorf("TableStore|SaveMessage json.Marshal err:%s", err)
	}
	err = i.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), msgBytes)
		if err != nil {
			return err
		}

		// 生成索引
		idxSettings, err := i.GetAvailableIndexWithTx(txn)
		if err != nil {
			return err
		}

		msgIDStr := cast.ToString(msg.MsgID)
		for _, setting := range idxSettings {
			idxKey := generateIdxKey(setting, msg)
			err := txn.Set([]byte(idxKey), []byte(msgIDStr))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("TableStore|SaveMessage db.Update err:%s", err)
	}
	return nil
}

// CreateIndex 创建索引
// TODO 思考：创建索引时需要协程异步，还是上 MDL 锁
func (i *TableStore) CreateIndex(idx Index) error {
	// 检查索引是否已经存在
	_, exist, err := i.GetIndex(idx.ColumnNames)
	if err != nil {
		return errors.Errorf("TableStore|CreateIndex i.GetIndex err:%s", err)
	}
	if exist {
		return errors.Errorf("TableStore|CreateIndex index %v already exist", idx.ColumnNames)
	}

	err = i.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek([]byte(_msgKeyPrefix)); it.ValidForPrefix([]byte(_msgKeyPrefix)); it.Next() {
			item := it.Item()

			valBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			temp := Message{}
			err = json.Unmarshal(valBytes, &temp)
			if err != nil {
				return err
			}

			idxKey := generateIdxKey(idx, temp)
			// 末尾补偿 msgId
			msgIdStr := cast.ToString(temp.MsgID)

			// 新增索引值
			err = txn.Set([]byte(idxKey), []byte(msgIdStr))
			if err != nil {
				return err
			}
		}

		// 插入索引配置
		idxSettingKey := getIdxSettingKey(idx.ColumnNames)
		idxVBytes, err := json.Marshal(idx)
		if err != nil {
			return err
		}
		err = txn.Set([]byte(idxSettingKey), idxVBytes)
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

func getIdxSettingKey(idxCols []string) string {
	idxName := strings.Join(idxCols, _indexSeparate)
	return fmt.Sprintf(_indexInfoKeyFormat, idxName)
}

// GetIndex 获取索引
func (i *TableStore) GetIndex(idxCols []string) (res Index, exist bool, err error) {
	key := getIdxSettingKey(idxCols)

	var vBytes []byte
	err = i.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		vBytes, err = item.ValueCopy(vBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return Index{}, false, nil
		}
		return Index{}, false, errors.Errorf("TableStore|GetIndex db.View err:%s", err)
	}

	err = json.Unmarshal(vBytes, &res)
	if err != nil {
		return Index{}, false, errors.Errorf("TableStore|GetIndex json.Unmarshal err:%s", err)
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

func (i *TableStore) PageMessage(pageSize int, lastMsgID uint64) ([]Message, uint64, error) {
	key := getMsgIDKey(lastMsgID + 1)

	result := make([]Message, 0, pageSize)
	var nextMsgID uint64

	err := i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(key)); it.ValidForPrefix([]byte(_msgKeyPrefix)); it.Next() {
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

			result = append(result, msgItem)
			if len(result) >= pageSize {
				lastMsgID = msgItem.MsgID
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, errors.Errorf("TableStore|PageMessage err:%s", err)
	}
	return result, nextMsgID, nil
}

// PrintAll 获取所有的键值
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
