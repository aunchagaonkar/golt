package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

const (
	testDuration = 10 * time.Second
	keysToWrite  = 20
)

func main() {
	log.Println("Test: Chaos Monkey & Consistency")
	log.Println("")

	testChaos()

	log.Println("")
	log.Println("All tests completed")
}

func testChaos() {
	log.Printf("Chaos Test: %v duration", testDuration)

	nodes := []struct {
		id      string
		address string
		peers   []string
	}{
		{"node1", "localhost:8001", []string{"localhost:8002", "localhost:8003"}},
		{"node2", "localhost:8002", []string{"localhost:8001", "localhost:8003"}},
		{"node3", "localhost:8003", []string{"localhost:8001", "localhost:8002"}},
	}

	nodeMap := make(map[string]*raft.Node)
	serverMap := make(map[string]*raft.Server)
	var mu sync.Mutex

	for _, cfg := range nodes {
		dir, _ := os.MkdirTemp("", "golt-chaos-"+cfg.id)
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		nodeMap[cfg.id] = node
		serverMap[cfg.id] = server
	}

	log.Println("Cluster started, waiting for leader...")
	time.Sleep(2 * time.Second)

	var stopChaos int32 = 0
	var writtenKeys sync.Map

	go func() {
		for atomic.LoadInt32(&stopChaos) == 0 {
			time.Sleep(time.Duration(rand.Intn(2000)+1000) * time.Millisecond)
			idx := rand.Intn(len(nodes))
			targetID := nodes[idx].id

			log.Printf("!!! CHAOS: Restarting %s !!!", targetID)

			mu.Lock()
			serverMap[targetID].Stop()

			newNode := nodeMap[targetID]
			newServer := raft.NewServer(newNode)
			if err := newServer.Start(); err != nil {
				log.Printf("Failed to restart %s: %v", targetID, err)
			} else {
				serverMap[targetID] = newServer
			}
			mu.Unlock()

			log.Printf("!!! %s Restarted !!!", targetID)
		}
	}()

	go func() {
		for i := 0; i < keysToWrite; i++ {
			if atomic.LoadInt32(&stopChaos) == 1 {
				break
			}

			key := fmt.Sprintf("k%d", i)
			val := fmt.Sprintf("v%d", i)

			for atomic.LoadInt32(&stopChaos) == 0 {
				mu.Lock()

				var leader *raft.Node
				for _, s := range serverMap {
					if s.Node().State() == raft.Leader && s.Node().CurrentTerm() > 0 {
						leader = s.Node()
						break
					}
				}
				mu.Unlock()

				if leader != nil {
					cmd := &pb.Command{Type: pb.CommandType_SET, Key: key, Value: val}
					idx := leader.AppendCommand(cmd)
					if idx > 0 {
						log.Printf("Write req: %s=%s (idx=%d, leader=%s)", key, val, idx, leader.ID())
						writtenKeys.Store(key, val)
						time.Sleep(500 * time.Millisecond) // slow down writes
						break
					}
				}

				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	time.Sleep(testDuration)
	atomic.StoreInt32(&stopChaos, 1)

	log.Println("")
	log.Println("Waiting for cluster to stabilize...")
	time.Sleep(5 * time.Second)

	log.Println("")
	log.Println("Verification")

	allKeys := make([]string, 0, keysToWrite)
	writtenKeys.Range(func(k, v interface{}) bool {
		allKeys = append(allKeys, k.(string))
		return true
	})

	log.Printf("Verifying %d potential keys across all nodes...", len(allKeys))

	consistent := true
	for _, key := range allKeys {
		vals := make(map[string][]string)

		mu.Lock()
		for id, s := range serverMap {
			val, found := s.Node().GetValue(key)
			vStr := "MISSING"
			if found {
				vStr = val
			}
			vals[vStr] = append(vals[vStr], id)
		}
		mu.Unlock()

		if len(vals) > 1 {
			log.Printf("✗ Inconsistency for key %s: %v", key, vals)
			consistent = false
		} else {
			for v, nodes := range vals {
				if v != "MISSING" {
					log.Printf("✓ Key %s = %s (agreed by %v)", key, v, nodes)
				} else {
					log.Printf("- Key %s not committed (missing on all)", key)
				}
			}
		}
	}

	if consistent {
		log.Println("✓ SUCCESS: All nodes have consistent state!")
	} else {
		log.Println("✗ FAILED: Inconsistencies detected.")
	}

	mu.Lock()
	stopAll(serverMap)
	mu.Unlock()
}

func stopAll(servers map[string]*raft.Server) {
	for _, s := range servers {
		s.Stop()
	}
}
