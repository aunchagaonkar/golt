package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
)

type Gateway struct {
	server     *raft.Server
	httpServer *http.Server
	httpAddr   string
}

func NewGateway(server *raft.Server, httpAddr string) *Gateway {
	return &Gateway{
		server:   server,
		httpAddr: httpAddr,
	}
}
func (g *Gateway) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/set", g.handleSet)
	mux.HandleFunc("/get", g.handleGet)
	mux.HandleFunc("/status", g.handleStatus)

	g.httpServer = &http.Server{
		Addr:    g.httpAddr,
		Handler: mux,
	}

	log.Printf("[Gateway] Starting HTTP server on %s", g.httpAddr)
	go func() {
		if err := g.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("[Gateway] HTTP server error: %v", err)
		}
	}()

	return nil
}

func (g *Gateway) Stop() error {
	if g.httpServer != nil {
		return g.httpServer.Close()
	}
	return nil
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

func (g *Gateway) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, http.StatusBadRequest, SetResponse{Error: "invalid request body"})
		return
	}

	if req.Key == "" {
		sendJSON(w, http.StatusBadRequest, SetResponse{Error: "key is required"})
		return
	}

	node := g.server.Node()

	if node.State() != raft.Leader {
		leaderAddr := node.LeaderAddr()
		if leaderAddr == "" {
			sendJSON(w, http.StatusServiceUnavailable, SetResponse{Error: "no leader"})
			return
		}

		resp, err := g.forwardSet(leaderAddr, req)
		if err != nil {
			sendJSON(w, http.StatusBadGateway, SetResponse{Error: fmt.Sprintf("forward failed: %v", err)})
			return
		}
		sendJSON(w, http.StatusOK, resp)
		return
	}

	cmd := &pb.Command{
		Type:  pb.CommandType_SET,
		Key:   req.Key,
		Value: req.Value,
	}

	index := node.AppendCommand(cmd)
	if index == 0 {
		sendJSON(w, http.StatusInternalServerError, SetResponse{Error: "append failed"})
		return
	}

	sendJSON(w, http.StatusOK, SetResponse{Success: true, Index: index})
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		sendJSON(w, http.StatusBadRequest, GetResponse{Error: "key query param required"})
		return
	}

	node := g.server.Node()
	value, found := node.GetValue(key)

	sendJSON(w, http.StatusOK, GetResponse{
		Key:   key,
		Value: value,
		Found: found,
	})
}

func (g *Gateway) handleStatus(w http.ResponseWriter, r *http.Request) {
	node := g.server.Node()

	sendJSON(w, http.StatusOK, StatusResponse{
		NodeID:   node.ID(),
		State:    node.State().String(),
		Term:     node.CurrentTerm(),
		Leader:   node.LeaderAddr(),
		IsLeader: node.State() == raft.Leader,
	})
}

func (g *Gateway) forwardSet(leaderAddr string, req SetRequest) (*SetResponse, error) {
	httpAddr := leaderAddr

	body, _ := json.Marshal(req)
	resp, err := http.Post("http://"+httpAddr+"/set", "application/json", bytes.NewReader(body))
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

func sendJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
