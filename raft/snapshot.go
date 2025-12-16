package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Snapshot struct {
	LastIncludedIndex uint64            `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64            `json:"lastIncludedTerm"`
	Data              map[string]string `json:"data"`
}

type SnapshotStore struct {
	dir string
	mu  sync.Mutex
}

func OpenSnapshotStore(dir string) *SnapshotStore {
	return &SnapshotStore{dir: dir}
}

func (s *SnapshotStore) SaveSnapshot(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	path := filepath.Join(s.dir, "snapshot.json")
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp failed: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}

	return nil
}

func (s *SnapshotStore) LoadSnapshot() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, "snapshot.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot failed: %w", err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return &snap, nil
}
