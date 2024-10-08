package fio

type IOType byte

const (
	StandardFileIO IOType = iota
	MemoryMap
)

const DataFilePerm = 0644

type IOManager interface {
	ReadAt(buf []byte, offset int64) (int, error)
	Write(buf []byte) (int, error)
	Size() (int64, error)
	Sync() error
	Close() error
}

func NewIOManager(typ IOType, filename string) (IOManager, error) {
	switch typ {
	case StandardFileIO:
		return NewFileIOManager(filename)
	case MemoryMap:
		return NewMMapIOManager(filename)
	default:
		panic("unsupported io type")
	}

}
