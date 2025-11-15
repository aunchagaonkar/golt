package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	node1Addr = "localhost:8001"
	node2Addr = "localhost:8002"
	node3Addr = "localhost:8003"
)

func main() {
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
		dir, _ := os.MkdirTemp("", "golt-ping-"+cfg.id)
		node := raft.NewNode(cfg.id, cfg.address, cfg.peers, dir)
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", cfg.id, err)
		}
		servers = append(servers, server)
	}

	time.Sleep(500 * time.Millisecond)
	log.Println("")
	log.Println("All nodes started successfully!")
	log.Println("")

	allAddresses := []string{node1Addr, node2Addr, node3Addr}
	successCount := 0
	totalTests := 0

	for _, cfg := range nodes {
		for _, targetAddr := range allAddresses {
			if targetAddr == cfg.address {
				continue
			}
			totalTests++

			log.Printf("Testing: %s -> %s", cfg.id, targetAddr)

			if err := pingNode(cfg.id, targetAddr); err != nil {
				log.Printf("FAILED: %v", err)
			} else {
				log.Printf("SUCCESS")
				successCount++
			}
		}
	}

	log.Println("")
	log.Println("Results")
	log.Printf("Passed: %d/%d tests", successCount, totalTests)

	if successCount == totalTests {
		log.Println("All ping tests passed!")
	} else {
		log.Println("Some tests failed")
	}

	log.Println("")
	log.Println("Shutting down nodes...")
	for _, server := range servers {
		server.Stop()
	}

	log.Println("Test completed.")
}

func pingNode(senderID, targetAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(targetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	resp, err := client.Ping(ctx, &pb.PingRequest{
		NodeId: senderID,
	})
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	log.Printf(" Response from %s: state=%s, term=%d", resp.NodeId, resp.State, resp.Term)
	return nil
}
