package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
	node       *node
	grpcServer *grpc.Server
	listener   net.Listener

	peerMu      sync.RWMutex
	peerClients map[string]pb.RaftServiceClient
	peerConns   map[string]*grpc.ClientConn
}

func NewServer(node *node) *Server {
	s := &Server{
		node:        node,
		peerClients: make(map[string]pb.RaftServiceClient),
		peerConns:   make(map[string]*grpc.ClientConn),
	}
	node.SetCallbacks(
		s.onBecomeCandidate,
		s.sendHeartbeats,
	)
	return s
}
func (s *Server) Start() error {
	address := s.node.Address()
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	s.listener = lis

	s.grpcServer = grpc.NewServer()
	pb.RegisterRaftServiceServer(s.grpcServer, s)
	log.Printf("[%s] Starting gRPC server on %s (state: %s, term: %d)", s.node.ID(), address, s.node.State(), s.node.CurrentTerm())

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("[%s] gRPC server error: %v", s.node.ID(), err)
		}
	}()

	go s.ConnectToPeers()

	s.node.StartElectionTimer()

	return nil
}

func (s *Server) ConnectToPeers() {
	peers := s.node.Peers()

	for _, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			log.Printf("[%s] Failed to create client for peer %s: %v", s.node.ID(), addr, err)
			continue
		}

		s.peerMu.Lock()
		s.peerClients[addr] = pb.NewRaftServiceClient(conn)
		s.peerConns[addr] = conn
		s.peerMu.Unlock()

		log.Printf("[%s] Client created for peer %s", s.node.ID(), addr)
	}
}
func (s *Server) Stop() {
	s.node.StopTimers()

	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	for addr, conn := range s.peerConns {
		conn.Close()
		delete(s.peerClients, addr)
		delete(s.peerConns, addr)
	}

	if s.grpcServer != nil {
		log.Printf("[%s] Stopping gRPC sverer", s.node.ID())
		s.grpcServer.GracefulStop()
	}
}

func (s *Server) Node() *node {
	return s.node
}

func (s *Server) onBecomeCandidate() {
	log.Printf("[%s] Candidate callback triggered", s.node.ID())
	// TODO

}
func (s *Server) sendHeartbeats() {
	s.peerMu.RLock()
	clients := make(map[string]pb.RaftServiceClient, len(s.peerClients))
	for addr, client := range s.peerClients {
		clients[addr] = client
	}
	s.peerMu.RUnlock()

	if len(clients) == 0 {
		return
	}

	currentTerm := s.node.CurrentTerm()
	leaderID := s.node.ID()

	log.Printf("[%s] Sending heartbeats to %d peers (term: %d)", leaderID, len(clients), currentTerm)

	for addr, client := range clients {
		go func(peerAddr string, c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req := &pb.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     leaderID,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			resp, err := c.AppendEntries(ctx, req)

			if err != nil {
				log.Printf("[%s] Heartbeat to %s failed: %v", leaderID, peerAddr, err)
				return
			}

			if resp.Term > currentTerm {
				log.Printf("[%s] Discovered higher term %d from %s, stepping down",
					leaderID, resp.Term, peerAddr)
				s.node.BecomeFollower(resp.Term)
			}
		}(addr, client)
	}
}

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.Printf("[%s] Received RequestVote from %s (term: %d, lastLogIndex: %d, lastLogTerm: %d)",
		s.node.ID(), req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	currentTerm := s.node.CurrentTerm()
	if req.Term > currentTerm {
		s.node.BecomeFollower(req.Term)
		currentTerm = req.Term
	}

	response := &pb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: false,
	}

	log.Printf("[%s] RequestVote response: term=%d, voteGranted=%v",
		s.node.ID(), response.Term, response.VoteGranted)

	return response, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	currentTerm := s.node.CurrentTerm()

	if len(req.Entries) == 0 {
		log.Printf("[%s] Received heartbeat from %s (term: %d)",
			s.node.ID(), req.LeaderId, req.Term)
	} else {
		log.Printf("[%s] Received AppendEntries from %s (term: %d, prevLogIndex: %d, entries: %d)",
			s.node.ID(), req.LeaderId, req.Term, req.PrevLogIndex, len(req.Entries))
	}

	if req.Term < currentTerm {
		log.Printf("[%s] Rejecting AppendEntries: leader term %d < current term %d",
			s.node.ID(), req.Term, currentTerm)
		return &pb.AppendEntriesResponse{Term: currentTerm, Success: false}, nil
	}

	if req.Term > currentTerm || s.node.State() != Follower {
		s.node.BecomeFollower(req.Term)
	}
	s.node.ResetElectionTimer()

	response := &pb.AppendEntriesResponse{
		Term:    s.node.CurrentTerm(),
		Success: true,
	}

	log.Printf("[%s] AppendEntries response: term=%d, success=%v",
		s.node.ID(), response.Term, response.Success)

	return response, nil
}

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Received Ping from %s", s.node.ID(), req.NodeId)

	response := &pb.PingResponse{
		NodeId: s.node.ID(),
		State:  s.node.State().ToProto(),
		Term:   s.node.CurrentTerm(),
	}

	log.Printf("[%s] Ping response: nodeId=%s, state=%s, term=%d",
		s.node.ID(), response.NodeId, response.State, response.Term)

	return response, nil
}
