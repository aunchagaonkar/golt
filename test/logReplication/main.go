package main

import (
	"log"
	"os"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {

	testLogReplication()

	log.Println("")
	log.Println("All tests completed")
}

func testLogReplication() {
	log.Println("Test: Log replication with 3 nodes")
	nodes := []struct {
		id      string
		address string
		peers   []string
	}{
		{"node1", "localhost:8001", []string{"localhost:8002", "localhost:8003"}},
		{"node2", "localhost:8002", []string{"localhost:8001", "localhost:8003"}},
		{"node3", "localhost:8003", []string{"localhost:8001", "localhost:8002"}},
	}

	servers := make([]*raft.Server, 0, 3)
	for _, cfg := range nodes {
		dir, _ := os.MkdirTemp("", "golt-log-rep-"+cfg.id)
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		servers = append(servers, server)
	}

	log.Println("")
	log.Println("Waiting for leader election...")
	time.Sleep(2 * time.Second)

	var leaderServer *raft.Server
	for _, s := range servers {
		if s.Node().State() == raft.Leader {
			leaderServer = s
			break
		}
	}

	if leaderServer == nil {
		log.Println("FAILED: No leader elected")
		stopAll(servers)
		return
	}

	log.Printf("Leader elected: %s", leaderServer.Node().ID())

	log.Println("")
	log.Println("Appending entries to leader...")

	cmd1 := &pb.Command{Type: pb.CommandType_SET, Key: "name", Value: "ameya"}
	cmd2 := &pb.Command{Type: pb.CommandType_SET, Key: "place", Value: "kolhapur"}
	cmd3 := &pb.Command{Type: pb.CommandType_DELETE, Key: "temp"}

	idx1 := leaderServer.Node().AppendCommand(cmd1)
	idx2 := leaderServer.Node().AppendCommand(cmd2)
	idx3 := leaderServer.Node().AppendCommand(cmd3)

	log.Printf("Appended entries at indices: %d, %d, %d", idx1, idx2, idx3)

	log.Println("")
	log.Println("Waiting for replication...")
	time.Sleep(1 * time.Second)

	log.Println("")
	log.Println("Checking log lengths:")
	allMatch := true
	expectedLen := leaderServer.Node().LogLength()

	for _, s := range servers {
		logLen := s.Node().LogLength()
		if logLen != expectedLen {
			allMatch = false
		}
		log.Printf("%s log length = %d (expected: %d)", s.Node().ID(), logLen, expectedLen)
	}

	if allMatch {
		log.Println("")
		log.Printf("SUCCESS: All nodes have %d log entries", expectedLen)
	} else {
		log.Println("")
		log.Println("FAILED: Log lengths don't match")
	}

	log.Println("")
	log.Println("Verifying entry contents:")
	for i := uint64(1); i <= expectedLen; i++ {
		leaderEntry := leaderServer.Node().GetLogEntry(i)
		allEntriesMatch := true

		for _, s := range servers {
			if s == leaderServer {
				continue
			}
			followerEntry := s.Node().GetLogEntry(i)
			if followerEntry == nil {
				allEntriesMatch = false
				continue
			}
			if followerEntry.Term != leaderEntry.Term ||
				followerEntry.Command.Key != leaderEntry.Command.Key {
				allEntriesMatch = false
			}
		}

		status := "CORRECT"
		if !allEntriesMatch {
			status = "WRONG"
		}
		log.Printf("  %s Entry %d: term=%d, cmd=%s %s=%s",
			status, i, leaderEntry.Term, leaderEntry.Command.Type,
			leaderEntry.Command.Key, leaderEntry.Command.Value)
	}

	log.Println("")
	log.Println("Shutting down nodes...")
	stopAll(servers)

	time.Sleep(100 * time.Millisecond)
}

func stopAll(servers []*raft.Server) {
	for _, s := range servers {
		s.Stop()
	}
}
