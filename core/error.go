package core

import "github.com/pkg/errors"

var (
	ErrParamMsgId         = errors.Errorf("msgId not allow nil")
	ErrColumnNotAllowNull = errors.Errorf("column not allow nil")
	ErrColumnTypeInvalid  = errors.Errorf("column type invalid")

	ErrTableAlreadyExist = errors.Errorf("table already exist")
	ErrTableNotFound     = errors.Errorf("table not found")

	ErrColumnAlreadyExist = errors.Errorf("column already exist") // column already exist when adding column

	ErrMessageColumnTypeInvalid = errors.Errorf("msg column type invalid") // 消息字段数据类型错误
)

type TableStoreErr struct {
	err error
	ext map[string]interface{}
}

func NewTableStoreErrWithExt(err error, ext map[string]interface{}) TableStoreErr {
	return TableStoreErr{err: err, ext: ext}
}

func NewTableStoreErr(err error) TableStoreErr {
	return TableStoreErr{err: err}
}

func (t TableStoreErr) Error() string {
	return t.err.Error()
}
