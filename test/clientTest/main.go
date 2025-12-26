package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aunchagaonkar/golt/api"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	testClient()

	log.Println("All tests completed")
}

func testClient() {
	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("golt-client-node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dirs[i] = dir
		defer os.RemoveAll(dir)
	}

	raftAddrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	httpAddrs := []string{"localhost:8101", "localhost:8102", "localhost:8103"}

	getPeers := func(idx int) []string {
		var peers []string
		for i, addr := range raftAddrs {
			if i != idx {
				peers = append(peers, addr)
			}
		}
		return peers
	}

	var servers []*raft.Server
	var gateways []*api.Gateway

	for i := 0; i < 3; i++ {
		node := raft.NewNode(fmt.Sprintf("node%d", i+1), raftAddrs[i], getPeers(i), dirs[i])
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Node %d start error: %v", i+1, err)
		}
		servers = append(servers, server)

		gateway := api.NewGateway(server, httpAddrs[i])
		if err := gateway.Start(); err != nil {
			log.Fatalf("Gateway %d start error: %v", i+1, err)
		}
		gateways = append(gateways, gateway)
	}

	defer func() {
		for _, gw := range gateways {
			gw.Stop()
		}
		for _, s := range servers {
			s.Stop()
		}
	}()

	log.Println("Waiting for leader election")
	time.Sleep(3 * time.Second)

	var leaderIdx int = -1
	for i, s := range servers {
		if s.Node().State() == raft.Leader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		log.Fatal("No leader elected!")
	}
	log.Printf("Leader is node%d (%s)", leaderIdx+1, httpAddrs[leaderIdx])

	log.Println("Test 1: Writing to leader")
	resp, err := httpSet(httpAddrs[leaderIdx], "key1", "value1")
	if err != nil {
		log.Fatalf("Write to leader failed: %v", err)
	}
	if !resp.Success {
		log.Fatalf("Write to leader not successful: %s", resp.Error)
	}
	log.Printf("✓ Write to leader succeeded (index=%d)", resp.Index)

	time.Sleep(500 * time.Millisecond)

	log.Println("Test 2: Reading from leader")
	getResp, err := httpGet(httpAddrs[leaderIdx], "key1")
	if err != nil {
		log.Fatalf("Read from leader failed: %v", err)
	}
	if !getResp.Found || getResp.Value != "value1" {
		log.Fatalf("Read from leader incorrect: found=%v, value=%s", getResp.Found, getResp.Value)
	}
	log.Println("✓ Read from leader succeeded")

	followerIdx := (leaderIdx + 1) % 3
	log.Printf("Test 3: Reading from follower (node%d)", followerIdx+1)
	getResp, err = httpGet(httpAddrs[followerIdx], "key1")
	if err != nil {
		log.Fatalf("Read from follower failed: %v", err)
	}
	if !getResp.Found || getResp.Value != "value1" {
		log.Fatalf("Read from follower incorrect: found=%v, value=%s", getResp.Found, getResp.Value)
	}
	log.Println("✓ Read from follower succeeded")

	log.Println("Test 4: Status endpoint")
	statusResp, err := httpStatus(httpAddrs[leaderIdx])
	if err != nil {
		log.Fatalf("Status failed: %v", err)
	}
	if !statusResp.IsLeader {
		log.Fatalf("Leader status incorrect")
	}
	log.Printf("✓ Status: nodeId=%s, state=%s, term=%d", statusResp.NodeID, statusResp.State, statusResp.Term)

	log.Println("✓ All client routing tests passed!")
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetResponse struct {
	Success bool   `json:"success"`
	Index   uint64 `json:"index,omitempty"`
	Error   string `json:"error,omitempty"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	Error string `json:"error,omitempty"`
}

type StatusResponse struct {
	NodeID   string `json:"nodeId"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	Leader   string `json:"leader,omitempty"`
	IsLeader bool   `json:"isLeader"`
}

func httpSet(addr, key, value string) (*SetResponse, error) {
	body, _ := json.Marshal(SetRequest{Key: key, Value: value})
	resp, err := http.Post("http://"+addr+"/set", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var setResp SetResponse
	if err := json.NewDecoder(resp.Body).Decode(&setResp); err != nil {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("decode failed: %v, body: %s", err, respBody)
	}
	return &setResp, nil
}

func httpGet(addr, key string) (*GetResponse, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/get?key=%s", addr, key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var getResp GetResponse
	json.NewDecoder(resp.Body).Decode(&getResp)
	return &getResp, nil
}

func httpStatus(addr string) (*StatusResponse, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/status", addr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var statusResp StatusResponse
	json.NewDecoder(resp.Body).Decode(&statusResp)
	return &statusResp, nil
}
