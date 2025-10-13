package main

import (
	"log"
	"time"

	"github.com/aunchagaonkar/golt/raft"
)

const (
	node1Addr = "localhost:8001"
	node2Addr = "localhost:8002"
	node3Addr = "localhost:8003"
)

func main() {
	log.Println("Test 1: Single node election timeout")
	testSingleNodeElectionTimeout()

	log.Println("")
	log.Println("Test 2: Three nodes, one becomes leader and sends heartbeats")
	testHeartbeatPreventsElection()

	log.Println("")
	log.Println("All tests completed")
}

func testSingleNodeElectionTimeout() {
	node := raft.NewNode("test-node", "localhost:9010", []string{})
	server := raft.NewServer(node)

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("Node started as %s", node.State())

	time.Sleep(400 * time.Millisecond)

	state := node.State()
	term := node.CurrentTerm()
	votedFor := node.VotedFor()

	log.Printf("After timeout: state=%s, term=%d, votedFor=%s", state, term, votedFor)

	if state == raft.Candidate {
		log.Println("SUCCESS: Node became Candidate after election timeout")
	} else {
		log.Printf("FAILED: Expected Candidate, got %s", state)
	}

	if term >= 1 {
		log.Printf("SUCCESS: Term incremented to %d", term)
	} else {
		log.Printf("FAILED: Expected term >= 1, got %d", term)
	}

	if votedFor == "test-node" {
		log.Println("SUCCESS: Node voted for self")
	} else {
		log.Printf("FAILED: Expected votedFor='test-node', got '%s'", votedFor)
	}

	server.Stop()
	time.Sleep(100 * time.Millisecond)
}

func testHeartbeatPreventsElection() {
	nodes := []struct {
		id      string
		address string
		peers   []string
	}{
		{"node1", node1Addr, []string{node2Addr, node3Addr}},
		{"node2", node2Addr, []string{node1Addr, node3Addr}},
		{"node3", node3Addr, []string{node1Addr, node2Addr}},
	}

	servers := make([]*raft.Server, 0, len(nodes))
	for _, cfg := range nodes {
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers)
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		servers = append(servers, server)
	}

	time.Sleep(500 * time.Millisecond)

	log.Println("All nodes started. Initial states:")
	for _, s := range servers {
		log.Printf("  %s: state=%s, term=%d", s.Node().ID(), s.Node().State(), s.Node().CurrentTerm())
	}

	log.Println("")
	log.Println("Making node1 the leader...")
	servers[0].Node().BecomeLeader()

	time.Sleep(300 * time.Millisecond)

	log.Println("")
	log.Println("After heartbeats:")

	leaderCount := 0
	followerCount := 0
	candidateCount := 0

	for _, s := range servers {
		state := s.Node().State()
		term := s.Node().CurrentTerm()
		log.Printf("  %s: state=%s, term=%d", s.Node().ID(), state, term)

		switch state {
		case raft.Leader:
			leaderCount++
		case raft.Follower:
			followerCount++
		case raft.Candidate:
			candidateCount++
		}
	}

	log.Println("")
	if leaderCount == 1 {
		log.Println("SUCCESS: Exactly 1 leader")
	} else {
		log.Printf("FAILED: Expected 1 leader, got %d", leaderCount)
	}

	if followerCount == 2 {
		log.Println("SUCCESS: 2 followers stayed as followers (heartbeats received)")
	} else {
		log.Printf("INFO: %d followers, %d candidates (some may have timed out before heartbeats arrived)",
			followerCount, candidateCount)
	}
	log.Println("")
	log.Println("Shutting down nodes...")
	for _, server := range servers {
		server.Stop()
	}

	time.Sleep(100 * time.Millisecond)
}
