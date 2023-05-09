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

		// check if msg valid
		if msg.MsgID == 0 {
			// generate msgId
			return NewTableStoreErrWithExt(ErrParamMsgId, nil)
		}
		for _, column := range tbInfo.Columns {
			// check not null column
			if !column.Nullable {
				_, exist := msg.ColumnValues[column.ColumnName]
				if !exist {
					return NewTableStoreErrWithExt(ErrColumnNotAllowNull, map[string]interface{}{
						"column": column,
					})
				}
			}

			// check type
			switch column.ColumnType {
			case ColumnTypeInteger:
				_, err := cast.ToInt64E(msg.ColumnValues[column.ColumnName])
				if err != nil {
					return NewTableStoreErrWithExt(ErrColumnTypeInvalid, map[string]interface{}{
						"column": column,
					})
				}
			case ColumnTypeDouble:
				_, err := cast.ToFloat64E(msg.ColumnValues[column.ColumnName])
				if err != nil {
					return NewTableStoreErrWithExt(ErrColumnTypeInvalid, map[string]interface{}{
						"column": column,
					})
				}
			}
		}

		// TODO check if msgId exist
		msgKey := GenerateMsgKey(tbInfo.TableId, msg.MsgID)

		// 首先保存主键
		err = txn.Set([]byte(msgKey), msgBytes)
		if err != nil {
			return errors.Errorf("TableStore|SaveMessage txn.Set err:%s", err)
		}

		// 生成索引
		indexes, err := i.GetAvailableIndexWithTx(txn, tbInfo.TableId)
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

type MessageQuery struct {
	TableName string      `json:"tableName"` // 表名
	MsgId     uint64      `json:"msgId"`     // 按照 msgId 查询
	Marker    uint64      `json:"marker"`
	PageSize  int         `json:"pageSize"`
	EqCond    []QueryCond `json:"eqCond"` // 条件设计
}

type QueryCondOper int

const (
	Gt QueryCondOper = iota + 1 // 大于
	Lt                          // 小于
	Eq                          // 等于
	Ge                          // 大于等于
	Le                          // 小于等于

	Null    // 为空
	NotNull // 非空
)

// QueryCond 查询条件
type QueryCond struct {
	ColumnName  string        `json:"columnName"`
	Oper        QueryCondOper // 比较运算
	ColumnValue string        `json:"columnValue"`
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
	colInfoMap := make(map[string]ColumnInfo, len(tbInfo.Columns))
	for _, column := range tbInfo.Columns {
		colInfoMap[column.ColumnName] = column
	}

	var seekKey string
	var seekKeyPrefix string
	// 优先采纳 msgId 的查询
	if query.MsgId != 0 {
		seekKey = GenerateMsgKey(tbInfo.TableId, query.MsgId)
		seekKeyPrefix = fmt.Sprintf(_primaryKeyPrefix, tbInfo.TableId)
	} else {
		// 提取所有的条件字段
		cols := make([]string, 0, len(query.EqCond))
		for _, cond := range query.EqCond {
			cols = append(cols, cond.ColumnName)
		}
		// 优化器选择索引
		plan := i.op.GeneratePlan(cols, tbInfo)
		// 生成索引
		seekKey = GenerateIndexKeyPrefixFromPlan(tbInfo, plan, query.EqCond)
		seekKeyPrefix = GenerateIndexKeyPrefix(tbInfo.TableId, plan.Idx.IndexId)
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

			// check if qualify marker
			if msgItem.MsgID <= query.Marker {
				continue
			}

			// check if msg valid
			if i.checkQueryCond(colInfoMap, query.EqCond, &msgItem) {
				continue
			}

			// check if over pageSize
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

func (i *TableStore) checkQueryCond(columnInfoMap map[string]ColumnInfo, conds []QueryCond, msg *Message) (skip bool) {
	for _, cond := range conds {
		v, notNull := msg.ColumnValues[cond.ColumnValue]
		if !notNull {
			if cond.Oper == NotNull {
				skip = true
				return
			} else if cond.Oper == Null {

			}
		}

		info, ok := columnInfoMap[cond.ColumnName]
		if !ok {
			// TODO 找不到列信息
		}

		switch info.ColumnType {
		case ColumnTypeInteger:
			vI, err := cast.ToInt64E(v)
			if err != nil {
				// TODO 数据库内部数据错误
			}

			cVI, err := cast.ToInt64E(cond.ColumnValue)
			if err != nil {
				// TODO 外部数据非法
			}

			// skip if not qualify
			switch cond.Oper {
			case Eq:
				if !(vI == cVI) {
					skip = true
					return
				}
			case Gt:
				if !(vI > cVI) {
					skip = true
					return
				}
			case Ge:
				if !(vI >= cVI) {
					skip = true
					return
				}
			case Le:
				if !(vI <= cVI) {
					skip = true
					return
				}
			case Lt:
				if !(vI < cVI) {
					skip = true
					return
				}
			}
		case ColumnTypeDouble:
			vI, err := cast.ToFloat64E(v)
			if err != nil {
				// TODO 数据库内部数据错误
			}

			cVI, err := cast.ToFloat64E(cond.ColumnValue)
			if err != nil {
				// TODO 外部数据非法
			}

			// skip if not qualify
			switch cond.Oper {
			case Eq:
				if !(vI == cVI) {
					skip = true
					return
				}
			case Gt:
				if !(vI > cVI) {
					skip = true
					return
				}
			case Ge:
				if !(vI >= cVI) {
					skip = true
					return
				}
			case Le:
				if !(vI <= cVI) {
					skip = true
					return
				}
			case Lt:
				if !(vI < cVI) {
					skip = true
					return
				}
			}
		case ColumnTypeString:
			vI := v
			cVI := cond.ColumnValue

			// skip if not qualify
			switch cond.Oper {
			case Eq:
				if !(vI == cVI) {
					skip = true
					return
				}
			case Gt:
				if !(vI > cVI) {
					skip = true
					return
				}
			case Ge:
				if !(vI >= cVI) {
					skip = true
					return
				}
			case Le:
				if !(vI <= cVI) {
					skip = true
					return
				}
			case Lt:
				if !(vI < cVI) {
					skip = true
					return
				}
			}
		default:
			// TODO 不支持的类型
		}
	}
	return false
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
