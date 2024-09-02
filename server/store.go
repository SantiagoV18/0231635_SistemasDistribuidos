package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Config struct {
	MaxStoreBytes int64
}

type Store struct {
	file   *os.File
	config Config
}

func NewStore(filePath string) (*Store, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &Store{file: f}, nil
}

func (s *Store) Append(record *api.Record) (uint64, error) {
	var offset uint64
	err := binary.Read(s.file, binary.LittleEndian, &offset)
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
		offset = 0
	}
	binary.LittleEndian.PutUint64(s.file.Seek(0, 0), offset+uint64(len(*record)))
	n, err := s.file.WriteAt([]byte(*record), offset)
	if err != nil {
		return 0, err
	}
	if n != len(*record) {
		return 0, fmt.Errorf("failed to write the entire record")
	}
	return offset, nil
}

func (s *Store) Read(off uint64) (*api.Record, error) {
	buf := make([]byte, 1024)
	n, err := s.file.ReadAt(buf, int64(off))
	if err != nil {
		return nil, err
	}
	buf = buf[:n]
	var record api.Record
	err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Store) Close() error {
	return s.file.Close()
}

func (s *Store) Remove() error {
	return os.Remove(s.file.Name())
}
