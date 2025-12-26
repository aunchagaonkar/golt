package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aunchagaonkar/golt/api"
	"github.com/aunchagaonkar/golt/raft"
	"github.com/aunchagaonkar/golt/shard"
)

func main() {
	testMultiRaftSharding()

	log.Println("=== All tests completed ===")
}

func testMultiRaftSharding() {
	dirs := make([]string, 6)
	for i := 0; i < 6; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("golt-shard-node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dirs[i] = dir
		defer os.RemoveAll(dir)
	}

	shardARaft := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	shardBRaft := []string{"localhost:9001", "localhost:9002", "localhost:9003"}
	shardAHTTP := []string{"localhost:8101", "localhost:8102", "localhost:8103"}
	shardBHTTP := []string{"localhost:9101", "localhost:9102", "localhost:9103"}

	log.Println("Starting Shard A (3 nodes)")
	serversA, gatewaysA := startShardGroup("shard-a", shardARaft, shardAHTTP, dirs[:3])
	defer stopShardGroup(serversA, gatewaysA)

	log.Println("Starting Shard B (3 nodes)")
	serversB, gatewaysB := startShardGroup("shard-b", shardBRaft, shardBHTTP, dirs[3:])
	defer stopShardGroup(serversB, gatewaysB)

	log.Println("Waiting for leader elections")
	time.Sleep(4 * time.Second)

	leaderAIdx := findLeader(serversA)
	leaderBIdx := findLeader(serversB)

	if leaderAIdx == -1 || leaderBIdx == -1 {
		log.Fatalf("Failed to elect leaders: shardA=%d, shardB=%d", leaderAIdx, leaderBIdx)
	}
	log.Printf("Shard A leader: node%d (%s)", leaderAIdx+1, shardAHTTP[leaderAIdx])
	log.Printf("Shard B leader: node%d (%s)", leaderBIdx+4, shardBHTTP[leaderBIdx])

	cluster := shard.NewShardCluster([]*shard.ShardGroup{
		{ID: "shard-a", HTTPAddr: shardAHTTP[leaderAIdx], RaftAddrs: shardARaft},
		{ID: "shard-b", HTTPAddr: shardBHTTP[leaderBIdx], RaftAddrs: shardBRaft},
	})

	log.Println("Writing keys to sharded cluster")

	keysA := []string{}
	keysB := []string{}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		resp, err := cluster.RouteSet(key, value)
		if err != nil {
			log.Fatalf("Failed to set %s: %v", key, err)
		}

		if !resp.Success {
			log.Fatalf("Set failed for %s: %s", key, resp.Error)
		}

		if resp.Shard == "shard-a" {
			keysA = append(keysA, key)
		} else {
			keysB = append(keysB, key)
		}
	}

	log.Printf("Keys in Shard A: %d", len(keysA))
	log.Printf("Keys in Shard B: %d", len(keysB))

	if len(keysA) == 0 || len(keysB) == 0 {
		log.Fatal("Keys not distributed across both shards!")
	}

	time.Sleep(1 * time.Second)

	log.Println("Verifying reads from sharded cluster")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		resp, err := cluster.RouteGet(key)
		if err != nil {
			log.Fatalf("Failed to get %s: %v", key, err)
		}

		if !resp.Found || resp.Value != expectedValue {
			log.Fatalf("Get failed for %s: found=%v, value=%s", key, resp.Found, resp.Value)
		}
	}

	log.Println("✓ All 20 keys verified successfully!")
	log.Println("✓ Multi-Raft sharding test passed!")
}

func startShardGroup(shardID string, raftAddrs, httpAddrs []string, dirs []string) ([]*raft.Server, []*api.Gateway) {
	var servers []*raft.Server
	var gateways []*api.Gateway

	for i := 0; i < len(raftAddrs); i++ {
		peers := make([]string, 0)
		for j, addr := range raftAddrs {
			if j != i {
				peers = append(peers, addr)
			}
		}

		nodeID := fmt.Sprintf("%s-node%d", shardID, i+1)
		node := raft.NewNode(nodeID, raftAddrs[i], peers, dirs[i])
		server := raft.NewServer(node)

		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", nodeID, err)
		}
		servers = append(servers, server)

		gateway := api.NewGateway(server, httpAddrs[i])
		if err := gateway.Start(); err != nil {
			log.Fatalf("Failed to start gateway for %s: %v", nodeID, err)
		}
		gateways = append(gateways, gateway)
	}

	return servers, gateways
}

func stopShardGroup(servers []*raft.Server, gateways []*api.Gateway) {
	for _, gw := range gateways {
		gw.Stop()
	}
	for _, s := range servers {
		s.Stop()
	}
}

func findLeader(servers []*raft.Server) int {
	for i, s := range servers {
		if s.Node().State() == raft.Leader {
			return i
		}
	}
	return -1
}
