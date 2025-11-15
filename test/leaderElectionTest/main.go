package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	testLeaderElection(5)

	log.Println("")
	log.Println("All tests completed")
}

func testLeaderElection(numNodes int) {
	log.Printf("Test: %d-node leader election with failover", numNodes)

	basePort := 8001
	nodes := make([]struct {
		id      string
		address string
		peers   []string
	}, numNodes)

	addresses := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		addresses[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	for i := 0; i < numNodes; i++ {
		nodes[i].id = fmt.Sprintf("node%d", i+1)
		nodes[i].address = addresses[i]
		nodes[i].peers = make([]string, 0, numNodes-1)
		for j := 0; j < numNodes; j++ {
			if i != j {
				nodes[i].peers = append(nodes[i].peers, addresses[j])
			}
		}
	}

	servers := make([]*raft.Server, 0, numNodes)
	for _, cfg := range nodes {
		dir, _ := os.MkdirTemp("", "golt-leader-"+cfg.id)
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

	var leaderIdx int = -1
	var leaderTerm uint64
	for i, s := range servers {
		state := s.Node().State()
		term := s.Node().CurrentTerm()
		log.Printf("  %s: state=%s, term=%d", s.Node().ID(), state, term)
		if state == raft.Leader {
			leaderIdx = i
			leaderTerm = term
		}
	}

	if leaderIdx >= 0 {
		log.Printf("")
		log.Printf("SUCCESS: Leader elected - %s (term: %d)", servers[leaderIdx].Node().ID(), leaderTerm)
	} else {
		log.Println("")
		log.Println("FAILED: No leader elected")
		stopAll(servers)
		return
	}

	log.Println("")
	log.Printf("Killing leader %s", servers[leaderIdx].Node().ID())
	servers[leaderIdx].Stop()

	log.Println("Waiting for new leader election...")
	startTime := time.Now()
	time.Sleep(2 * time.Second)

	var newLeaderIdx int = -1
	var newLeaderTerm uint64
	log.Println("")
	log.Println("After failover:")
	for i, s := range servers {
		if i == leaderIdx {
			log.Printf("  %s: STOPPED", s.Node().ID())
			continue
		}
		state := s.Node().State()
		term := s.Node().CurrentTerm()
		log.Printf("  %s: state=%s, term=%d", s.Node().ID(), state, term)
		if state == raft.Leader {
			newLeaderIdx = i
			newLeaderTerm = term
		}
	}

	elapsed := time.Since(startTime)

	if newLeaderIdx >= 0 && newLeaderIdx != leaderIdx {
		log.Println("")
		log.Printf("SUCCESS: New leader elected - %s (term: %d)", servers[newLeaderIdx].Node().ID(), newLeaderTerm)
		log.Printf("SUCCESS: Failover completed in %.2f seconds", elapsed.Seconds())

		if newLeaderTerm > leaderTerm {
			log.Println("SUCCESS: New term is higher than old term")
		}
	} else {
		log.Println("")
		log.Println("FAILED: No new leader elected after failover")
	}
	leaderCount := 0
	for i, s := range servers {
		if i == leaderIdx {
			continue
		}
		if s.Node().State() == raft.Leader {
			leaderCount++
		}
	}

	if leaderCount == 1 {
		log.Println("SUCCESS: Exactly one leader in the cluster")
	} else {
		log.Printf("FAILED: Expected 1 leader, got %d", leaderCount)
	}

	log.Println("")
	log.Println("Shutting down remaining nodes...")
	for i, s := range servers {
		if i != leaderIdx {
			s.Stop()
		}
	}

	time.Sleep(100 * time.Millisecond)
}

func stopAll(servers []*raft.Server) {
	for _, s := range servers {
		s.Stop()
	}
}
