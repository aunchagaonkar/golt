package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	id := flag.String("id", "", "Unique node ID (required)")
	address := flag.String("address", "", "Address to listen on, e.g. localhost:8001 (required)")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses, e.g. localhost:8002,localhost:8003")
	flag.Parse()

	if *id == "" {
		log.Fatal("Error: -id flag is required")
	}
	if *address == "" {
		log.Fatal("Error: -address flag is required")
	}
	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
		for i, peer := range peerList {
			peerList[i] = strings.TrimSpace(peer)
		}
	}

	log.Printf("Starting Golt node %s on %s", *id, *address)
	log.Printf("Peers: %v", peerList)

	node := raft.NewNode(*id, *address, peerList)
	server := raft.NewServer(node)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("Received signal %v, shutting down...", sig)

	server.Stop()
	log.Printf("Node %s stopped", *id)
}
