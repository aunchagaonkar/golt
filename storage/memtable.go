package storage

import (
	"sort"
	"sync"
)

type Entry struct {
	Value   string
	Deleted bool
}

type MemTable struct {
	mu   sync.RWMutex
	data map[string]Entry
	size int // size in bytes
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]Entry),
		size: 0,
	}
}

func (m *MemTable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldEntry, exists := m.data[key]
	if exists {
		m.size -= len(key) + len(oldEntry.Value) + 1 // 1 for bool
	}
	m.data[key] = Entry{Value: value, Deleted: false}
	m.size += len(key) + len(value) + 1
}

func (m *MemTable) Get(key string) (string, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	return entry.Value, ok, entry.Deleted
}

func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldEntry, exists := m.data[key]
	if exists {
		m.size -= len(key) + len(oldEntry.Value) + 1
	}
	m.data[key] = Entry{Value: "", Deleted: true}
	m.size += len(key) + 1
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *MemTable) Flush() ([]string, map[string]Entry) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys, m.data
}
