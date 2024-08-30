package bitcask

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrKeyNotFound       = errors.New("key not exist")
	ErrDataFileNotFound  = errors.New("data file not found")
	ErrIndexUpdateFailed = errors.New("failed to update index")
)
