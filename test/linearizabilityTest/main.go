package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aunchagaonkar/golt/api"
	"github.com/aunchagaonkar/golt/logger"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	logger.Init("linearizability-test")

	testLinearizability()

	log.Println("Test completed")
}

type Operation struct {
	Type      string
	Key       string
	Value     int
	Timestamp time.Time
	NodeID    string
	Success   bool
}

type History struct {
	mu  sync.Mutex
	ops []Operation
}

func (h *History) Add(op Operation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, op)
}

func (h *History) GetOps() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]Operation, len(h.ops))
	copy(result, h.ops)
	return result
}

func testLinearizability() {

	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("golt-linear-node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dirs[i] = dir
		defer os.RemoveAll(dir)
	}

	raftAddrs := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	httpAddrs := []string{"localhost:9001", "localhost:9002", "localhost:9003"}

	var servers []*raft.Server
	var gateways []*api.Gateway

	for i := 0; i < 3; i++ {
		peers := make([]string, 0)
		for j, addr := range raftAddrs {
			if j != i {
				peers = append(peers, addr)
			}
		}

		node := raft.NewNode(fmt.Sprintf("node%d", i+1), raftAddrs[i], peers, dirs[i])
		server := raft.NewServer(node)
		if err := server.Start(); err != nil {
			log.Fatalf("Server %d start failed: %v", i+1, err)
		}
		servers = append(servers, server)

		gateway := api.NewGateway(server, httpAddrs[i])
		if err := gateway.Start(); err != nil {
			log.Fatalf("Gateway %d start failed: %v", i+1, err)
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

	leaderIdx := -1
	for i, s := range servers {
		if s.Node().State() == raft.Leader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		log.Fatal("No leader elected!")
	}
	log.Printf("Leader: node%d", leaderIdx+1)

	numWriters := 3
	numReaders := 5
	numOpsPerWriter := 20
	testKey := "counter"

	history := &History{}
	var counter int64 = 0
	var wg sync.WaitGroup

	log.Printf("Starting %d writers, %d ops each", numWriters, numOpsPerWriter)
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < numOpsPerWriter; i++ {
				newVal := int(atomic.AddInt64(&counter, 1))
				start := time.Now()

				success := httpSet(httpAddrs[leaderIdx], testKey, strconv.Itoa(newVal))

				history.Add(Operation{
					Type:      "write",
					Key:       testKey,
					Value:     newVal,
					Timestamp: start,
					NodeID:    fmt.Sprintf("writer-%d", writerID),
					Success:   success,
				})

				time.Sleep(10 * time.Millisecond)
			}
		}(w)
	}

	log.Printf("Starting %d readers", numReaders)
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			nodeIdx := readerID % 3
			for i := 0; i < numOpsPerWriter*2; i++ {
				start := time.Now()
				val := httpGet(httpAddrs[nodeIdx], testKey)

				intVal := 0
				if val != "" {
					intVal, _ = strconv.Atoi(val)
				}

				history.Add(Operation{
					Type:      "read",
					Key:       testKey,
					Value:     intVal,
					Timestamp: start,
					NodeID:    fmt.Sprintf("reader-%d-node%d", readerID, nodeIdx+1),
					Success:   true,
				})

				time.Sleep(5 * time.Millisecond)
			}
		}(r)
	}

	wg.Wait()
	log.Println("All operations completed.")

	violations := analyzeHistory(history)

	if len(violations) == 0 {
		log.Println("NO LINEARIZABILITY VIOLATIONS DETECTED!")
		log.Println("SUCCESS All reads returned values consistent with write order.")
	} else {
		log.Printf("FAIL | FOUND %d LINEARIZABILITY VIOLATIONS!", len(violations))
		for i, v := range violations {
			if i < 10 {
				log.Printf("  Violation: %s", v)
			}
		}
	}

	ops := history.GetOps()
	writes := 0
	reads := 0
	for _, op := range ops {
		if op.Type == "write" {
			writes++
		} else {
			reads++
		}
	}
	log.Printf("Total operations: %d writes, %d reads", writes, reads)
}

func analyzeHistory(h *History) []string {
	ops := h.GetOps()

	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Timestamp.Before(ops[j].Timestamp)
	})

	var violations []string
	maxWrittenValue := 0
	maxWriteTime := time.Time{}

	for _, op := range ops {
		if op.Type == "write" && op.Success {
			if op.Value > maxWrittenValue {
				maxWrittenValue = op.Value
				maxWriteTime = op.Timestamp
			}
		} else if op.Type == "read" {
			if op.Timestamp.After(maxWriteTime.Add(100*time.Millisecond)) && op.Value < maxWrittenValue-1 {
				violations = append(violations, fmt.Sprintf(
					"Read got %d at %v, but write of %d was confirmed at %v (by %s)",
					op.Value, op.Timestamp.Format("15:04:05.000"),
					maxWrittenValue, maxWriteTime.Format("15:04:05.000"),
					op.NodeID,
				))
			}
		}
	}

	return violations
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type GetResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

func httpSet(addr, key, value string) bool {
	body, _ := json.Marshal(SetRequest{Key: key, Value: value})
	resp, err := http.Post("http://"+addr+"/set", "application/json", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var setResp SetResponse
	json.NewDecoder(resp.Body).Decode(&setResp)
	return setResp.Success
}

func httpGet(addr, key string) string {
	resp, err := http.Get(fmt.Sprintf("http://%s/get?key=%s", addr, key))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var getResp GetResponse
	json.Unmarshal(body, &getResp)
	if getResp.Found {
		return getResp.Value
	}
	return ""
}
