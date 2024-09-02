package segment

import (
	"fmt"
	"path/filepath"

	"github.com/your/project/log"
)

type Segment struct {
	store *store.Store
	index *index.Index
	base  uint64
	next  uint64
}

func NewSegment(dir string, off uint64, c log.Config) (*Segment, error) {
	s := &Segment{
		base: off,
		next: off,
	}
	var err error
	// Verificar que el archivo de store se cree correctamente
	s.store, err = store.NewStore(filepath.Join(dir, fmt.Sprintf("store_%d.log", off)))
	if err != nil {
		return nil, err
	}
	// Verificar que el archivo de index se cree correctamente
	s.index, err = index.NewIndex(dir, off, c)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Segment) Append(record *api.Record) (uint64, error) {
	pos, err := s.store.Append(record)
	if err != nil {
		return 0, err
	}
	// Verificar que el offset se esté actualizando correctamente
	err = s.index.Write(s.next, pos)
	if err != nil {
		return 0, err
	}
	s.next++
	return s.next - 1, nil
}

func (s *Segment) Read(off uint64) (*api.Record, error) {
	pos, err := s.index.Read(off)
	if err != nil {
		return nil, err
	}
	// Verificar que el offset se esté transformando correctamente
	return s.store.Read(pos)
}

func (s *Segment) IsMaxed() bool {
	return s.next-s.base >= uint64(s.store.Config.MaxStoreBytes)
}

func (s *Segment) Close() error {
	err := s.store.Close()
	if err != nil {
		return err
	}
	return s.index.Close()
}

func (s *Segment) Remove() error {
	err := s.store.Remove()
	if err != nil {
		return err
	}
	return s.index.Remove()
}
