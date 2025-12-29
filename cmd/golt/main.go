package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aunchagaonkar/golt/api"
	"github.com/aunchagaonkar/golt/logger"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	id := flag.String("id", "", "Unique node ID (required)")
	address := flag.String("address", "", "Raft address to listen on, e.g. localhost:7001 (required)")
	httpAddr := flag.String("http", "", "HTTP API address, e.g. localhost:8001 (optional)")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses, e.g. localhost:7002,localhost:7003")
	dataDir := flag.String("data", "", "Directory for storing data")
	flag.Parse()

	if *id == "" {
		log.Fatal("Error: -id flag is required")
	}
	if *address == "" {
		log.Fatal("Error: -address flag is required")
	}
	if *dataDir == "" {
		*dataDir = "/tmp/golt-" + *id
	}

	logger.Init(*id)

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
		for i, peer := range peerList {
			peerList[i] = strings.TrimSpace(peer)
		}
	}

	log.Printf("Starting Golt node %s on %s", *id, *address)
	log.Printf("Peers: %v", peerList)
	log.Printf("Data Directory: %s", *dataDir)

	node := raft.NewNode(*id, *address, peerList, *dataDir)
	server := raft.NewServer(node)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	var gateway *api.Gateway
	if *httpAddr != "" {
		gateway = api.NewGateway(server, *httpAddr)
		if err := gateway.Start(); err != nil {
			log.Fatalf("Failed to start HTTP gateway: %v", err)
		}
		log.Printf("HTTP API available at http://%s", *httpAddr)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("Received signal %v, shutting down...", sig)

	if gateway != nil {
		gateway.Stop()
	}

	server.Stop()
	log.Printf("Node %s stopped", *id)
}
