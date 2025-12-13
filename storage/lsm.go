package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type LSMStore struct {
	dir           string
	mu            sync.RWMutex
	memTable      *MemTable
	memTableLimit int
	sstables      []*SSTable
}

func OpenLSMStore(dir string, memTableLimit int) (*LSMStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %w", err)
	}

	store := &LSMStore{
		dir:           dir,
		memTable:      NewMemTable(),
		memTableLimit: memTableLimit,
		sstables:      make([]*SSTable, 0),
	}

	if err := store.loadSSTables(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *LSMStore) loadSSTables() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sst") {
			files = append(files, filepath.Join(s.dir, e.Name()))
		}
	}
	sort.Strings(files)

	for i := len(files) - 1; i >= 0; i-- {
		s.sstables = append(s.sstables, OpenSSTable(files[i]))
	}

	log.Printf("LSMStore loaded %d SSTables", len(s.sstables))
	return nil
}

func (s *LSMStore) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.memTable.Put(key, value)

	if s.memTable.Size() >= s.memTableLimit {
		return s.flushMemTable()
	}
	return nil
}

func (s *LSMStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.memTable.Delete(key)

	if s.memTable.Size() >= s.memTableLimit {
		return s.flushMemTable()
	}
	return nil
}

func (s *LSMStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, found, deleted := s.memTable.Get(key)
	if found {
		if deleted {
			return "", false
		}
		return val, true
	}

	for _, sst := range s.sstables {
		val, found, deleted := sst.Search(key)
		if found {
			if deleted {
				return "", false
			}
			return val, true
		}
	}

	return "", false
}

func (s *LSMStore) flushMemTable() error {
	filename := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
	path := filepath.Join(s.dir, filename)

	keys, data := s.memTable.Flush()

	if err := WriteSSTable(path, keys, data); err != nil {
		return err
	}

	s.sstables = append([]*SSTable{OpenSSTable(path)}, s.sstables...)
	s.memTable = NewMemTable()

	log.Printf("Flushed MemTable to %s", filename)
	return nil
}

func (s *LSMStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMemTable()
	return nil
}
