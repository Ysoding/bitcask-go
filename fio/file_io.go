package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(filename string) (*FileIO, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

func (f *FileIO) Read(buf []byte) (int, error) {
	return f.fd.Read(buf)
}

func (f *FileIO) ReadAt(buf []byte, offset int64) (int, error) {
	return f.fd.ReadAt(buf, offset)
}

func (f *FileIO) Write(buf []byte) (int, error) {
	return f.fd.Write(buf)
}

func (f *FileIO) Size() (int64, error) {
	stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}
