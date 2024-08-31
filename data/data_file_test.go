package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ysoding/bitcask/fio"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}
