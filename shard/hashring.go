package shard

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type HashRing struct {
	mu       sync.RWMutex
	ring     []uint32
	nodes    map[uint32]string
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	if replicas < 1 {
		replicas = 100
	}
	return &HashRing{
		ring:     make([]uint32, 0),
		nodes:    make(map[uint32]string),
		replicas: replicas,
	}
}

func (h *HashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (h *HashRing) AddNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hash := h.hash(virtualKey)

		if _, exists := h.nodes[hash]; !exists {
			h.ring = append(h.ring, hash)
			h.nodes[hash] = nodeID
		}
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newRing := make([]uint32, 0, len(h.ring))
	for _, hash := range h.ring {
		if h.nodes[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(h.nodes, hash)
		}
	}
	h.ring = newRing
}

func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := h.hash(key)

	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx >= len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}

func (h *HashRing) GetShardID(key string) string {
	return h.GetNode(key)
}

func (h *HashRing) NodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	unique := make(map[string]bool)
	for _, nodeID := range h.nodes {
		unique[nodeID] = true
	}
	return len(unique)
}

func (h *HashRing) Nodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	unique := make(map[string]bool)
	for _, nodeID := range h.nodes {
		unique[nodeID] = true
	}

	result := make([]string, 0, len(unique))
	for nodeID := range unique {
		result = append(result, nodeID)
	}
	return result
}
