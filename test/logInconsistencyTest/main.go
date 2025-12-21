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

	testLogRepair()

	log.Println("")
	log.Println("All tests completed")
}

func testLogRepair() {
	log.Println("Test: Follower Catch-up after Partition")

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

	for _, cfg := range nodes {
		dir, _ := os.MkdirTemp("", "golt-logincon"+cfg.id)
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		nodeMap[cfg.id] = node
		serverMap[cfg.id] = server
	}

	log.Println("Waiting for leader election...")
	time.Sleep(2 * time.Second)

	var leaderID string
	for id, n := range nodeMap {
		if n.State() == raft.Leader {
			leaderID = id
			break
		}
	}

	if leaderID == "" {
		log.Println("FAILED: No leader elected")
		stopAll(serverMap)
		return
	}
	log.Printf("Leader elected: %s", leaderID)

	followerID := "node3"
	if leaderID == "node3" {
		followerID = "node2"
	}

	log.Println("Appending initial entry...")
	cmd0 := &pb.Command{Type: pb.CommandType_SET, Key: "init", Value: "val"}
	nodeMap[leaderID].AppendCommand(cmd0)
	time.Sleep(1 * time.Second)

	if nodeMap[followerID].LogLength() != 1 {
		log.Printf("FAILED: Follower %s failed to replicate initial entry", followerID)
		stopAll(serverMap)
		return
	}
	log.Printf("Follower %s replicated initial entry", followerID)

	log.Printf("Stopping follower %s to simulate partition...", followerID)
	serverMap[followerID].Stop()

	log.Println("Appending 5 new entries to leader while follower is down...")
	for i := 1; i <= 5; i++ {
		cmd := &pb.Command{
			Type:  pb.CommandType_SET,
			Key:   fmt.Sprintf("k%d", i),
			Value: fmt.Sprintf("v%d", i),
		}
		idx := nodeMap[leaderID].AppendCommand(cmd)
		log.Printf("Appended entry at index %d", idx)
	}

	time.Sleep(2 * time.Second)

	log.Printf("Restarting follower %s...", followerID)
	newServer := raft.NewServer(nodeMap[followerID])
	if err := newServer.Start(); err != nil {
		log.Fatalf("Failed to restart %s: %v", followerID, err)
	}
	serverMap[followerID] = newServer

	log.Println("Waiting for follower to catch up (log repair)...")
	time.Sleep(3 * time.Second)

	expectedLen := uint64(6)
	actualLen := nodeMap[followerID].LogLength()

	if actualLen == expectedLen {
		log.Printf("SUCCESS: Follower %s caught up to log length %d", followerID, actualLen)
	} else {
		log.Printf("FAILED: Follower %s has log length %d (expected %d)", followerID, actualLen, expectedLen)

		log.Printf("  Leader log length: %d", nodeMap[leaderID].LogLength())
		entries := nodeMap[followerID].GetLogEntries(1)
		for _, e := range entries {
			log.Printf("Follower entry: index=%d term=%d", e.Index, e.Term)
		}
	}

	stopAll(serverMap)
}

func stopAll(servers map[string]*raft.Server) {
	for _, s := range servers {
		s.Stop()
	}
}
