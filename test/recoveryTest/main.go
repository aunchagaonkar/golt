package main

import (
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	testRecovery()

	log.Println("")
	log.Println("All tests completed")
}

func testRecovery() {
	log.Println("Test: Persist 100 keys, Crash, Recover")

	numKeys := 100

	nodes := []struct {
		id      string
		address string
		peers   []string
	}{
		{"node1", "localhost:8001", []string{"localhost:8002", "localhost:8003"}},
		{"node2", "localhost:8002", []string{"localhost:8001", "localhost:8003"}},
		{"node3", "localhost:8003", []string{"localhost:8001", "localhost:8002"}},
	}

	dirs := make(map[string]string)
	for _, cfg := range nodes {
		dir, err := os.MkdirTemp("", "golt-recovery-"+cfg.id)
		if err != nil {
			log.Fatalf("Failed to create temp dir: %v", err)
		}
		dirs[cfg.id] = dir
		defer os.RemoveAll(dir)
	}

	startCluster := func() (map[string]*raft.Server, map[string]*raft.Node) {
		servers := make(map[string]*raft.Server)
		nodeMap := make(map[string]*raft.Node)
		for _, cfg := range nodes {
			dir := dirs[cfg.id]
			node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
			server := raft.NewServer(node, "")
			if err := server.Start(); err != nil {
				log.Fatalf("Failed to start %s: %v", cfg.id, err)
			}
			servers[cfg.id] = server
			nodeMap[cfg.id] = node
		}
		return servers, nodeMap
	}

	log.Println("Starting cluster...")
	servers, nodeMap := startCluster()

	log.Println("Waiting for leader...")
	time.Sleep(2 * time.Second)

	var leaderID string
	for id, n := range nodeMap {
		if n.State() == raft.Leader {
			leaderID = id
			break
		}
	}
	if leaderID == "" {
		log.Fatal("No leader elected")
	}
	log.Printf("Leader is %s", leaderID)

	log.Printf("Writing %d keys to leader...", numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		cmd := &pb.Command{Type: pb.CommandType_SET, Key: key, Value: val}

		idx := nodeMap[leaderID].AppendCommand(cmd)
		if idx == 0 {
			log.Fatalf("Failed to append key %s", key)
		}
	}
	log.Println("Waiting for data to replicate and commit")
	time.Sleep(2 * time.Second)

	if val, ok := nodeMap[leaderID].GetValue("k99"); !ok || val != "v99" {
		log.Fatal("Data write failed or not applied yet")
	}
	log.Println("Data verified on leader before crash.")

	log.Println("CRASHING ALL NODES")
	for _, s := range servers {
		s.Stop()
	}
	time.Sleep(1 * time.Second)

	log.Println("Restarting all nodes")
	servers, nodeMap = startCluster()

	log.Println("Waiting for cluster recovery")
	time.Sleep(3 * time.Second)

	leaderID = ""
	for id, n := range nodeMap {
		if n.State() == raft.Leader {
			leaderID = id
			break
		}
	}
	if leaderID == "" {
		log.Println("Warning: No leader elected yet, but followers might have data.")
	} else {
		log.Printf("New Leader is %s", leaderID)
	}

	log.Println("Writing one new key to force commit")
	cmd := &pb.Command{Type: pb.CommandType_SET, Key: "k-new", Value: "v-new"}
	idx := nodeMap[leaderID].AppendCommand(cmd)
	if idx == 0 {
		log.Fatal("Failed to append new key")
	}

	log.Println("Waiting for commit")
	time.Sleep(2 * time.Second)

	log.Println("Verifying 100 keys on all nodes")

	successCount := 0
	for _, cfg := range nodes {
		node := nodeMap[cfg.id]
		missing := 0
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("k%d", i)
			expectedVal := fmt.Sprintf("v%d", i)
			val, ok := node.GetValue(key)
			if !ok || val != expectedVal {
				missing++
			}
		}

		if missing == 0 {
			log.Printf("✓ Node %s recovered all %d keys", cfg.id, numKeys)
			successCount++
		} else {
			log.Printf("✗ Node %s missing %d keys", cfg.id, missing)
		}
	}

	if successCount == 3 {
		log.Println("✓ SUCCESS: Full Cluster Recovery Verified")
	} else {
		log.Println("✗ FAILED: some nodes lost data")
	}

	for _, s := range servers {
		s.Stop()
	}
}
