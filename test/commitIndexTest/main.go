package main

import (
	"log"
	"os"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {

	testCommitIndex()

	log.Println("")
	log.Println("All tests completed")
}

func testCommitIndex() {
	log.Println("Test: Commit Index")

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
		dir, _ := os.MkdirTemp("", "golt-commit-"+cfg.id)
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
		server := raft.NewServer(node, "")
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		servers = append(servers, server)
	}
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

	cmd := &pb.Command{Type: pb.CommandType_SET, Key: "k", Value: "v"}
	idx := leaderServer.Node().AppendCommand(cmd)
	log.Printf("Appended entry at index %d", idx)

	log.Println("Waiting for replication and commit...")
	time.Sleep(1 * time.Second)

	leaderCommit := leaderServer.Node().CommitIndex()
	if leaderCommit >= idx {
		log.Printf("Leader commitIndex is %d (expected >= %d)", leaderCommit, idx)
	} else {
		log.Printf("Leader commitIndex is %d (expected >= %d)", leaderCommit, idx)
	}

	paramsMatched := true
	for _, s := range servers {
		if s == leaderServer {
			continue
		}
		commitLimit := s.Node().CommitIndex()
		if commitLimit >= idx {
			log.Printf("Follower %s commitIndex is %d", s.Node().ID(), commitLimit)
		} else {
			log.Printf("Follower %s commitIndex is %d (expected >= %d)", s.Node().ID(), commitLimit, idx)
			paramsMatched = false
		}
	}

	if paramsMatched && leaderCommit >= idx {
		log.Println("SUCCESS: Commit index propagated to cluster")
	} else {
		log.Println("FAILED: Commit index not propagated correctly")
	}

	stopAll(servers)
}

func stopAll(servers []*raft.Server) {
	for _, s := range servers {
		s.Stop()
	}
}
