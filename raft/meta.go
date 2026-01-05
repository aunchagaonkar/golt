package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Metadata struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
}

type MetaStore struct {
	mu   sync.Mutex
	path string
}

func OpenMetaStore(dir string) *MetaStore {
	return &MetaStore{
		path: filepath.Join(dir, "raftState.json"),
	}
}

func (m *MetaStore) Load() (*Metadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return &Metadata{CurrentTerm: 0, VotedFor: ""}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	if len(data) == 0 {
		return &Metadata{CurrentTerm: 0, VotedFor: ""}, nil
	}

	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

func (m *MetaStore) Save(meta *Metadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	tmpPath := m.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp metadata: %w", err)
	}

	if err := os.Rename(tmpPath, m.path); err != nil {
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	return nil
}
