package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"go-tablestore/api"
	"go-tablestore/config"
	"strings"
)

const (
	_msgKeyPrefix          = "msg:"
	_indexKeyPrefix        = "idx_vals:"
	_indexSettingKeyPrefix = "idx_setting:"

	_msgKeyFormat          = _msgKeyPrefix + "%020d"       // 一级索引配置
	_indexKeyFormat        = _indexKeyPrefix + "%s:%s"     // 二级索引配置
	_indexSettingKeyFormat = _indexSettingKeyPrefix + "%s" // 索引配置

	_nullField = "NULL"

	_indexSeparate = "-"
)

type IMDB struct {
	msgDB *badger.DB // TODO 索引和消息是否要存放在同一个 LSM Tree 中？
}

func NewBadgerDb(dbCfg config.DBConfig) (*IMDB, error) {
	msgDir := "./msg"
	if dbCfg.DataDir != "" {
		msgDir = dbCfg.DataDir
	}

	msgDBOpt := badger.DefaultOptions(msgDir)

	msgDB, err := badger.Open(msgDBOpt)
	if err != nil {
		return nil, err
	}

	return &IMDB{msgDB: msgDB}, nil
}

func getMsgIDKey(msgID uint64) string {
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, msgID)
	return fmt.Sprintf(_msgKeyFormat, msgID)
}

func generateIdxKey(idxSetting IndexSetting, msg Message) string {
	idxName := strings.Join(idxSetting.Fields, _indexSeparate)
	idxValRaw := make([]string, 0, len(idxSetting.Fields)+1)

	needAppendMsgId := true
	for _, field := range idxSetting.Fields {
		if strings.ToLower(field) == "msgid" {
			idxValRaw = append(idxValRaw, cast.ToString(msg.MsgID))
			needAppendMsgId = false
		} else {
			fieldVal, ok := msg.Columns[field]
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
func (i *IMDB) GetDataByMsgId(msgID uint64) (Message, error) {
	key := getMsgIDKey(msgID)

	var result []byte
	err := i.msgDB.View(func(txn *badger.Txn) error {
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
		return Message{}, errors.Errorf("IMDB|GetDataByMsgId msgDB.View err:%s", err)
	}

	msgRes := Message{}
	err = json.Unmarshal(result, &msgRes)
	if err != nil {
		return Message{}, errors.Errorf("IMDB|json.Unmarshal err:%s", err)
	}
	return Message{}, nil
}

// SaveMessage 增加消息
func (i *IMDB) SaveMessage(msg Message) error {
	key := getMsgIDKey(msg.MsgID)
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return errors.Errorf("IMDB|SaveMessage json.Marshal err:%s", err)
	}
	err = i.msgDB.Update(func(txn *badger.Txn) error {
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
		return errors.Errorf("IMDB|SaveMessage msgDB.Update err:%s", err)
	}
	return nil
}

// CreateIndex 创建索引
// TODO 思考：创建索引时需要协程异步，还是上 MDL 锁
func (i *IMDB) CreateIndex(idx IndexSetting) error {
	// 检查索引是否已经存在
	_, exist, err := i.GetIndex(idx.Fields)
	if err != nil {
		return errors.Errorf("IMDB|CreateIndex i.GetIndex err:%s", err)
	}
	if exist {
		return errors.Errorf("IMDB|CreateIndex index %v already exist", idx.Fields)
	}

	err = i.msgDB.Update(func(txn *badger.Txn) error {
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
		idxSettingKey := getIdxSettingKey(idx.Fields)
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
		return errors.Errorf("IMDB|CreateIndex msgDB.Update err:%s", err)
	}
	return nil
}

// ListIndex 列出所有的索引
func (i *IMDB) ListIndex() (res []IndexSetting, err error) {
	err = i.msgDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek([]byte(_indexSettingKeyPrefix)); it.ValidForPrefix([]byte(_indexSettingKeyPrefix)); it.Next() {
			item := it.Item()
			vBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			temp := IndexSetting{}
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
	return fmt.Sprintf(_indexSettingKeyFormat, idxName)
}

// GetIndex 获取索引
func (i *IMDB) GetIndex(idxCols []string) (res IndexSetting, exist bool, err error) {
	key := getIdxSettingKey(idxCols)

	var vBytes []byte
	err = i.msgDB.View(func(txn *badger.Txn) error {
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
			return IndexSetting{}, false, nil
		}
		return IndexSetting{}, false, errors.Errorf("IMDB|GetIndex msgDB.View err:%s", err)
	}

	err = json.Unmarshal(vBytes, &res)
	if err != nil {
		return IndexSetting{}, false, errors.Errorf("IMDB|GetIndex json.Unmarshal err:%s", err)
	}
	return
}

// GetAvailableIndex 获取可选择的索引列表
func (i *IMDB) GetAvailableIndex() (res []IndexSetting, err error) {
	tx := i.msgDB.NewTransaction(false)
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
func (i *IMDB) GetAvailableIndexWithTx(txn *badger.Txn) ([]IndexSetting, error) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var result []IndexSetting
	for it.Seek([]byte(_indexSettingKeyPrefix)); it.ValidForPrefix([]byte(_indexSettingKeyPrefix)); it.Next() {
		item := it.Item()
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, errors.Errorf("IMDB|GetAvailableIndexWithTx item.ValueCopy err:%s", err)
		}

		temp := IndexSetting{}

		err = json.Unmarshal(valueBytes, &temp)
		if err != nil {
			return nil, errors.Errorf("IMDB|GetAvailableIndexWithTx json.Unmarshal err:%s", err)
		}

		if temp.Enable {
			result = append(result, temp)
		}
	}
	return result, nil
}

// RemoveIndexSetting 删除掉索引信息
func (i *IMDB) RemoveIndexSetting(idxSetting IndexSetting) error {
	return nil
}

// DropIndex 卸载索引
// TODO 方案设计，将索引设置为不可用状态，然后异步卸载所有索引值
func (i *IMDB) DropIndex(idx IndexSetting) error {
	return nil
}

// SearchMessage 搜索消息
func (i *IMDB) SearchMessage(option ...api.SearchOption) ([]Message, error) {
	var result []Message
	return result, nil
}

func (i *IMDB) PageMessage(pageSize int, lastMsgID uint64) ([]Message, uint64, error) {
	key := getMsgIDKey(lastMsgID + 1)

	result := make([]Message, 0, pageSize)
	var nextMsgID uint64

	err := i.msgDB.View(func(txn *badger.Txn) error {
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
		return nil, 0, errors.Errorf("IMDB|PageMessage err:%s", err)
	}
	return result, nextMsgID, nil
}

// PrintAll 获取所有的键值
func (i *IMDB) PrintAll(prefix string) error {
	err := i.msgDB.View(func(txn *badger.Txn) error {
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
func (i *IMDB) CloseGracefully() error {
	err := i.msgDB.Flatten(0)
	if err != nil {
		// TODO log
		return err
	}
	err = i.msgDB.Close()
	if err != nil {
		return err
	}
	return nil
}

// ClearAllData 清空所有数据
func (i *IMDB) ClearAllData() error {
	return i.msgDB.DropAll()
}