package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrKeyNotFound            = errors.New("key not exist")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrDatabaseIsUsing        = errors.New("database directory is used by another process")
	ErrDataDirectoryCorrupted = errors.New("database directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrMergeRatioUnreached    = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough disk space for merge")
)
