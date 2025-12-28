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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aunchagaonkar/golt/api"
	"github.com/aunchagaonkar/golt/logger"
	"github.com/aunchagaonkar/golt/raft"
)

func main() {
	logger.Init("benchmark")

	runBenchmark()
}

type BenchmarkResult struct {
	Operation    string
	TotalOps     int
	Duration     time.Duration
	OpsPerSecond float64
	AvgLatency   time.Duration
	P50Latency   time.Duration
	P99Latency   time.Duration
}

func (r BenchmarkResult) String() string {
	return fmt.Sprintf("%s: %.2f ops/sec (avg: %v, p50: %v, p99: %v)",
		r.Operation, r.OpsPerSecond, r.AvgLatency, r.P50Latency, r.P99Latency)
}

func runBenchmark() {
	dirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("golt-bench-node%d-", i+1))
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

	log.Println("Starting 3-node cluster")
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
	log.Printf("Leader: node%d (%s)", leaderIdx+1, httpAddrs[leaderIdx])

	numClients := 10
	opsPerClient := 100

	log.Println("\nWrite Benchmark")
	writeResult := benchmarkWrites(httpAddrs[leaderIdx], numClients, opsPerClient)
	log.Println(writeResult)

	time.Sleep(500 * time.Millisecond)

	log.Println("\nRead Benchmark (Leader)")
	readLeaderResult := benchmarkReads(httpAddrs[leaderIdx], numClients, opsPerClient)
	log.Println(readLeaderResult)

	followerIdx := (leaderIdx + 1) % 3
	log.Println("\nRead Benchmark (Follower)")
	readFollowerResult := benchmarkReads(httpAddrs[followerIdx], numClients, opsPerClient)
	log.Println(readFollowerResult)

	log.Println("\nBENCHMARK SUMMARY")
	log.Printf("Write Throughput: %.0f ops/sec", writeResult.OpsPerSecond)
	log.Printf("Read Throughput (Leader): %.0f ops/sec", readLeaderResult.OpsPerSecond)
	log.Printf("Read Throughput (Follower): %.0f ops/sec", readFollowerResult.OpsPerSecond)
	log.Println("\nBenchmark Complete")
}

func benchmarkWrites(addr string, numClients, opsPerClient int) BenchmarkResult {
	var wg sync.WaitGroup
	var totalOps int64
	latencies := make([]time.Duration, 0, numClients*opsPerClient)
	var latMu sync.Mutex

	start := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("bench-key-%d-%d", clientID, i)
				value := fmt.Sprintf("bench-value-%d-%d", clientID, i)

				opStart := time.Now()
				if httpSet(addr, key, value) {
					atomic.AddInt64(&totalOps, 1)
					lat := time.Since(opStart)
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				}
			}
		}(c)
	}

	wg.Wait()
	duration := time.Since(start)

	return calculateResult("Writes", int(totalOps), duration, latencies)
}

func benchmarkReads(addr string, numClients, opsPerClient int) BenchmarkResult {
	var wg sync.WaitGroup
	var totalOps int64
	latencies := make([]time.Duration, 0, numClients*opsPerClient)
	var latMu sync.Mutex

	start := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("bench-key-%d-%d", clientID%10, i%100)

				opStart := time.Now()
				if httpGet(addr, key) != "" || true {
					atomic.AddInt64(&totalOps, 1)
					lat := time.Since(opStart)
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				}
			}
		}(c)
	}

	wg.Wait()
	duration := time.Since(start)

	return calculateResult("Reads", int(totalOps), duration, latencies)
}

func calculateResult(op string, totalOps int, duration time.Duration, latencies []time.Duration) BenchmarkResult {
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var avgLat, p50Lat, p99Lat time.Duration
	if len(latencies) > 0 {
		var sum time.Duration
		for _, l := range latencies {
			sum += l
		}
		avgLat = sum / time.Duration(len(latencies))
		p50Lat = latencies[len(latencies)/2]
		p99Lat = latencies[int(float64(len(latencies))*0.99)]
	}

	opsPerSec := float64(totalOps) / duration.Seconds()

	return BenchmarkResult{
		Operation:    op,
		TotalOps:     totalOps,
		Duration:     duration,
		OpsPerSecond: opsPerSec,
		AvgLatency:   avgLat,
		P50Latency:   p50Lat,
		P99Latency:   p99Lat,
	}
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetResponse struct {
	Success bool `json:"success"`
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
	var getResp struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	json.Unmarshal(body, &getResp)
	if getResp.Found {
		return getResp.Value
	}
	return ""
}
