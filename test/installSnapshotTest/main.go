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
	testInstallSnapshot()

	log.Println("All tests completed")
}

func testInstallSnapshot() {
	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("golt-snap-node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dirs[i] = dir
		defer os.RemoveAll(dir)
	}

	addrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}

	getPeers := func(idx int) []string {
		var peers []string
		for i, addr := range addrs {
			if i != idx {
				peers = append(peers, addr)
			}
		}
		return peers
	}

	log.Println("Starting node1 and node2")

	node1 := raft.NewNode("node1", addrs[0], getPeers(0), dirs[0])
	server1 := raft.NewServer(node1, "")
	if err := server1.Start(); err != nil {
		log.Fatalf("Server1 start failed: %v", err)
	}
	defer server1.Stop()

	node2 := raft.NewNode("node2", addrs[1], getPeers(1), dirs[1])
	server2 := raft.NewServer(node2, "")
	if err := server2.Start(); err != nil {
		log.Fatalf("Server2 start failed: %v", err)
	}
	defer server2.Stop()

	log.Println("Waiting for leader election...")
	time.Sleep(3 * time.Second)

	var leader *raft.Node
	if node1.State() == raft.Leader {
		leader = node1
	} else if node2.State() == raft.Leader {
		leader = node2
	} else {
		log.Fatal("No leader elected!")
	}
	log.Printf("Leader: %s", leader.ID())

	log.Println("Writing 1500 entries to leader")
	for i := 0; i < 1500; i++ {
		cmd := &pb.Command{
			Type:  pb.CommandType_SET,
			Key:   fmt.Sprintf("k%d", i),
			Value: "val",
		}
		if idx := leader.AppendCommand(cmd); idx == 0 {
			log.Fatalf("Append failed at %d", i)
		}
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Println("Waiting for replication and snapshot")
	time.Sleep(5 * time.Second)

	lastIncIdx := leader.LastIncludedIndex()
	log.Printf("Leader lastIncludedIndex: %d", lastIncIdx)
	if lastIncIdx == 0 {
		log.Fatal("Snapshot was never taken on leader")
	}

	log.Println("Starting node3 - lagging node")
	node3 := raft.NewNode("node3", addrs[2], getPeers(2), dirs[2])
	server3 := raft.NewServer(node3, "")
	if err := server3.Start(); err != nil {
		log.Fatalf("Server3 start failed: %v", err)
	}
	defer server3.Stop()

	log.Println("Waiting for node3 connections and InstallSnapshot")
	time.Sleep(10 * time.Second)

	node3LastIncIdx := node3.LastIncludedIndex()
	log.Printf("node3 lastIncludedIndex: %d", node3LastIncIdx)

	log.Println("Verifying node3 state...")

	val, ok := node3.GetValue("k10")
	if !ok || val != "val" {
		log.Fatalf("node3 missing k10 (snapshot install failed)")
	}

	val, ok = node3.GetValue("k1400")
	if !ok || val != "val" {
		log.Fatalf("node3 missing k1400")
	}

	log.Println("✓ node3 caught up via InstallSnapshot successfully!")
}
