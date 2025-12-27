package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/aunchagaonkar/golt/logger"
	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/raft"
	"github.com/google/uuid"
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

	logger.Info().Str("addr", g.httpAddr).Msg("Starting HTTP gateway")
	go func() {
		if err := g.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server error")
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
	NodeID      string `json:"nodeId"`
	State       string `json:"state"`
	Term        uint64 `json:"term"`
	CommitIndex uint64 `json:"commitIndex"`
	LogLength   uint64 `json:"logLength"`
	Leader      string `json:"leader,omitempty"`
	IsLeader    bool   `json:"isLeader"`
}

func (g *Gateway) handleSet(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()[:8]
	log := logger.WithRequestID(requestID)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Warn().Err(err).Msg("Invalid request body")
		sendJSON(w, http.StatusBadRequest, SetResponse{Error: "invalid request body"})
		return
	}

	if req.Key == "" {
		sendJSON(w, http.StatusBadRequest, SetResponse{Error: "key is required"})
		return
	}
	log.Info().Str("key", req.Key).Msg("SET request")

	node := g.server.Node()

	if node.State() != raft.Leader {
		leaderAddr := node.LeaderAddr()
		if leaderAddr == "" {
			log.Warn().Msg("No leader available")
			sendJSON(w, http.StatusServiceUnavailable, SetResponse{Error: "no leader"})
			return
		}

		log.Info().Str("leader", leaderAddr).Msg("Forwarding to leader")
		resp, err := g.forwardSet(leaderAddr, req)
		if err != nil {
			log.Error().Err(err).Msg("Forward failed")
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
		log.Error().Msg("Append failed")
		sendJSON(w, http.StatusInternalServerError, SetResponse{Error: "append failed"})
		return
	}

	log.Info().Uint64("index", index).Msg("SET success")
	sendJSON(w, http.StatusOK, SetResponse{Success: true, Index: index})
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()[:8]
	log := logger.WithRequestID(requestID)

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		sendJSON(w, http.StatusBadRequest, GetResponse{Error: "key query param required"})
		return
	}

	log.Info().Str("key", key).Msg("GET request")

	node := g.server.Node()
	value, found := node.GetValue(key)

	log.Info().Bool("found", found).Msg("GET result")

	sendJSON(w, http.StatusOK, GetResponse{
		Key:   key,
		Value: value,
		Found: found,
	})
}

func (g *Gateway) handleStatus(w http.ResponseWriter, r *http.Request) {
	node := g.server.Node()

	sendJSON(w, http.StatusOK, StatusResponse{
		NodeID:      node.ID(),
		State:       node.State().String(),
		Term:        node.CurrentTerm(),
		CommitIndex: node.CommitIndex(),
		LogLength:   node.LogLength(),
		Leader:      node.LeaderAddr(),
		IsLeader:    node.State() == raft.Leader,
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
