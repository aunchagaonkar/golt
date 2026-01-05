package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	testSnapshot()

	log.Println("All tests completed")
}

func testSnapshot() {
	dir, err := os.MkdirTemp("", "golt-snap-test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	log.Printf("Using temp dir: %s", dir)

	nodeID := "node1"
	addr := "localhost:8001"
	peers := []string{}
	startNode := func() (*raft.Server, *raft.Node) {
		n := raft.NewNode(nodeID, addr, peers, dir)
		s := raft.NewServer(n, "")
		if err := s.Start(); err != nil {
			log.Fatalf("Server start failed: %v", err)
		}
		return s, n
	}

	log.Println("Starting Node...")
	server, node := startNode()
	time.Sleep(2 * time.Second)
	if node.State() != raft.Leader {
		log.Fatalf("Node did not become leader. State: %s", node.State())
	}
	log.Println("Node is Leader.")

	log.Println("Writing 1500 entries...")
	for i := 0; i < 1500; i++ {
		cmd := &pb.Command{
			Type:  pb.CommandType_SET,
			Key:   fmt.Sprintf("k%d", i),
			Value: "val",
		}
		if idx := node.AppendCommand(cmd); idx == 0 {
			log.Fatalf("Append failed at %d", i)
		}
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Println("Waiting for processing")
	time.Sleep(3 * time.Second)

	logPath := filepath.Join(dir, "raft.log")
	info, err := os.Stat(logPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("WAL file size: %d bytes", info.Size())
	snapPath := filepath.Join(dir, "snapshot.json")
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		log.Fatal("snapshot.json does not exist!")
	}
	log.Println("snapshot.json found.")

	log.Println("Stopping Node")
	server.Stop()
	time.Sleep(1 * time.Second)

	log.Println("Restarting Node")
	server, node = startNode()

	log.Println("Waiting for node to become leader and catch up...")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		log.Fatalf("Node failed to become leader after restart")
	}

	log.Println("Writing new key to force commit")
	cmd := &pb.Command{
		Type:  pb.CommandType_SET,
		Key:   "k-new",
		Value: "val-new",
	}
	if idx := node.AppendCommand(cmd); idx == 0 {
		log.Fatalf("Append new key failed")
	}

	log.Println("Verifying state after restart")

	foundEarly := false
	foundLate := false

	for i := 0; i < 50; i++ {
		val, ok := node.GetValue("k10")
		if ok && val == "val" {
			foundEarly = true
		}

		val, ok = node.GetValue("k1499")
		if ok && val == "val" {
			foundLate = true
		}

		if foundEarly && foundLate {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !foundEarly {
		log.Fatalf("Missing early key k10 (Snapshot recovery failed)")
	}
	if !foundLate {
		log.Fatalf("Missing late key k1499 (Log recovery failed)")
	}

	log.Println("✓ State recovered successfully from Snapshot + WAL")

	server.Stop()
}
