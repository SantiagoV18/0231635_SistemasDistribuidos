package server

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/your/project/log"
)

type Index struct {
	file *os.File
}

func NewIndex(dir string, off uint64, c log.Config) (*Index, error) {
	filePath := filepath.Join(dir, fmt.Sprintf("index_%d.log", off))
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &Index{file: f}, nil
}

func (i *Index) Write(off uint64, pos uint64) error {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf, off)
	binary.LittleEndian.PutUint64(buf[8:], pos)
	_, err := i.file.WriteAt(buf, int64(off))
	return err
}

func (i *Index) Read(off uint64) (uint64, error) {
	buf := make([]byte, 16)
	_, err := i.file.ReadAt(buf, int64(off))
	if err != nil {
		return 0, err
	}
	pos := binary.LittleEndian.Uint64(buf[8:])
	return pos, nil
}

func (i *Index) Close() error {
	return i.file.Close()
}

func (i *Index) Remove() error {
	return os.Remove(i.file.Name())
}
