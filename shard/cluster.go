package shard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type ShardGroup struct {
	ID        string
	HTTPAddr  string
	RaftAddrs []string
}

type ShardCluster struct {
	mu     sync.RWMutex
	ring   *HashRing
	shards map[string]*ShardGroup
}

func NewShardCluster(groups []*ShardGroup) *ShardCluster {
	ring := NewHashRing(100)
	shards := make(map[string]*ShardGroup)

	for _, g := range groups {
		ring.AddNode(g.ID)
		shards[g.ID] = g
	}

	return &ShardCluster{
		ring:   ring,
		shards: shards,
	}
}

func (c *ShardCluster) GetShardForKey(key string) *ShardGroup {
	c.mu.RLock()
	defer c.mu.RUnlock()

	shardID := c.ring.GetNode(key)
	if shardID == "" {
		return nil
	}
	return c.shards[shardID]
}

func (c *ShardCluster) UpdateShardLeader(shardID, httpAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if shard, ok := c.shards[shardID]; ok {
		shard.HTTPAddr = httpAddr
	}
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetResponse struct {
	Success bool   `json:"success"`
	Index   uint64 `json:"index,omitempty"`
	Error   string `json:"error,omitempty"`
	Shard   string `json:"shard,omitempty"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	Error string `json:"error,omitempty"`
	Shard string `json:"shard,omitempty"`
}

func (c *ShardCluster) RouteSet(key, value string) (*SetResponse, error) {
	shard := c.GetShardForKey(key)
	if shard == nil {
		return nil, fmt.Errorf("no shard found for key: %s", key)
	}

	if shard.HTTPAddr == "" {
		return nil, fmt.Errorf("shard %s has no leader", shard.ID)
	}

	body, _ := json.Marshal(SetRequest{Key: key, Value: value})
	resp, err := http.Post("http://"+shard.HTTPAddr+"/set", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request to shard %s failed: %w", shard.ID, err)
	}
	defer resp.Body.Close()

	var setResp SetResponse
	if err := json.NewDecoder(resp.Body).Decode(&setResp); err != nil {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("decode failed: %v, body: %s", err, respBody)
	}

	setResp.Shard = shard.ID
	return &setResp, nil
}

func (c *ShardCluster) RouteGet(key string) (*GetResponse, error) {
	shard := c.GetShardForKey(key)
	if shard == nil {
		return nil, fmt.Errorf("no shard found for key: %s", key)
	}

	if shard.HTTPAddr == "" {
		return nil, fmt.Errorf("shard %s has no leader", shard.ID)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/get?key=%s", shard.HTTPAddr, key))
	if err != nil {
		return nil, fmt.Errorf("request to shard %s failed: %w", shard.ID, err)
	}
	defer resp.Body.Close()

	var getResp GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		return nil, fmt.Errorf("decode failed: %v", err)
	}

	getResp.Shard = shard.ID
	return &getResp, nil
}

func (c *ShardCluster) Shards() []*ShardGroup {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*ShardGroup, 0, len(c.shards))
	for _, s := range c.shards {
		result = append(result, s)
	}
	return result
}
