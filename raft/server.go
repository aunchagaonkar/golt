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

	for _, peerAddr := range peers {
		go func(addr string) {
			for i := 0; i < 5; i++ {
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("[%s] Failed to create client for peer %s (attempt %d): %v", s.node.ID(), addr, i+1, err)
					if i < 4 {
						time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
					}
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, err = pb.NewRaftServiceClient(conn).Ping(ctx, &pb.PingRequest{NodeId: s.node.ID()})
				cancel()
				if err != nil {
					log.Printf("[%s] Ping test failed for peer %s (attempt %d): %v", s.node.ID(), addr, i+1, err)
					conn.Close()
					if i < 4 {
						time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
					}
					continue
				}

				s.peerMu.Lock()
				s.peerClients[addr] = pb.NewRaftServiceClient(conn)
				s.peerConns[addr] = conn
				s.peerMu.Unlock()

				log.Printf("[%s] Connected to peer %s", s.node.ID(), addr)
				return
			}
			log.Printf("[%s] Failed to connect to peer %s after 5 attempts", s.node.ID(), addr)
		}(peerAddr)
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
		log.Printf("[%s] Stopping gRPC server", s.node.ID())
		s.grpcServer.GracefulStop()
	}
}

func (s *Server) Node() *node {
	return s.node
}

func (s *Server) onBecomeCandidate() {
	s.peerMu.RLock()
	clients := make(map[string]pb.RaftServiceClient, len(s.peerClients))
	for addr, client := range s.peerClients {
		clients[addr] = client
	}
	s.peerMu.RUnlock()

	currentTerm := s.node.CurrentTerm()
	candidateID := s.node.ID()
	lastLogIndex := s.node.LastLogIndex()
	lastLogTerm := s.node.LastLogTerm()
	needed := s.node.MajoritySize()

	log.Printf("[%s] Starting election for term %d, need %d votes", candidateID, currentTerm, needed)

	votes := 1

	if votes >= needed {
		log.Printf("[%s] Single node cluster, becoming Leader for term %d", candidateID, currentTerm)
		s.node.BecomeLeader()
		return
	}

	if len(clients) == 0 {
		log.Printf("[%s] No connected peers to request votes from", candidateID)
		return
	}

	type voteResult struct {
		peerAddr    string
		term        uint64
		voteGranted bool
		err         error
	}

	results := make(chan voteResult, len(clients))

	for addr, client := range clients {
		go func(peerAddr string, c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			req := &pb.RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			resp, err := c.RequestVote(ctx, req)
			if err != nil {
				results <- voteResult{peerAddr: peerAddr, err: err}
				return
			}
			results <- voteResult{peerAddr: peerAddr, term: resp.Term, voteGranted: resp.VoteGranted}
		}(addr, client)
	}
	votesReceived := 0
	for votesReceived < len(clients) {
		result := <-results
		votesReceived++

		if s.node.State() != Candidate || s.node.CurrentTerm() != currentTerm {
			log.Printf("[%s] No longer a Candidate, stopping election processing", candidateID)
			return
		}
		if result.err != nil {
			log.Printf("[%s] Vote request to %s failed: %v", candidateID, result.peerAddr, result.err)
			continue
		}
		if result.term > currentTerm {
			log.Printf("[%s] Discovered higher term %d from %s, stepping down", candidateID, result.term, result.peerAddr)
			s.node.BecomeFollower(result.term)
			return
		}
		if result.voteGranted {
			votes++
			log.Printf("[%s] Received vote from %s (total votes: %d)", candidateID, result.peerAddr, votes)

			if votes >= needed {
				log.Printf("[%s] Received majority votes, becoming Leader for term %d", candidateID, currentTerm)
				s.node.BecomeLeader()
				return
			}

		} else {
			log.Printf("[%s] Vote denied by %s", candidateID, result.peerAddr)
		}
	}
	log.Printf("[%s] Election concluded for term %d with %d votes (needed %d)", candidateID, currentTerm, votes, needed)
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
	commitIndex := s.node.CommitIndex()

	log.Printf("[%s] Sending heartbeats to %d peers (term: %d)", leaderID, len(clients), currentTerm)

	for addr, client := range clients {
		go func(peerAddr string, c pb.RaftServiceClient) {

			nextIndex := s.node.GetNextIndex(peerAddr)
			entries := s.node.GetLogEntries(nextIndex)
			prevLogIndex, prevLogTerm := s.node.GetPrevLogInfo(nextIndex)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req := &pb.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			resp, err := c.AppendEntries(ctx, req)

			if err != nil {
				log.Printf("[%s] Heartbeat to %s failed: %v", leaderID, peerAddr, err)
				return
			}

			if resp.Term > currentTerm {
				log.Printf("[%s] Discovered higher term %d from %s, stepping down", leaderID, resp.Term, peerAddr)
				s.node.BecomeFollower(resp.Term)
				return
			}

			if resp.Success {
				if len(entries) > 0 {
					lastEntry := entries[len(entries)-1]
					s.node.SetNextIndex(peerAddr, lastEntry.Index+1)
					s.node.SetMatchIndex(peerAddr, lastEntry.Index)
					log.Printf("[%s] Peer %s replicated entries up to index %d", leaderID, peerAddr, lastEntry.Index)
				}
			} else {
				if nextIndex > 1 {
					s.node.SetNextIndex(peerAddr, nextIndex-1)
					log.Printf("[%s] Peer %s log mismatch, decremented nextIndex to %d", leaderID, peerAddr, nextIndex-1)
				}
			}
		}(addr, client)
	}
}

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.Printf("[%s] Received RequestVote from %s (term: %d, lastLogIndex: %d, lastLogTerm: %d)",
		s.node.ID(), req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	voteGranted, currentTerm := s.node.CanGrantVote(req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	if voteGranted {
		s.node.ResetElectionTimer()
	}

	response := &pb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}

	log.Printf("[%s] RequestVote response: term=%d, voteGranted=%v",
		s.node.ID(), response.Term, response.VoteGranted)

	return response, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	currentTerm := s.node.CurrentTerm()
	currentState := s.node.State()

	isHeartbeat := len(req.Entries) == 0
	if isHeartbeat {
		log.Printf("[%s] Received heartbeat from %s (term: %d, prevLogIndex: %d)",
			s.node.ID(), req.LeaderId, req.Term, req.PrevLogIndex)
	} else {
		log.Printf("[%s] Received AppendEntries from %s (term: %d, prevLogIndex: %d, entries: %d)",
			s.node.ID(), req.LeaderId, req.Term, req.PrevLogIndex, len(req.Entries))
	}

	if req.Term < currentTerm {
		log.Printf("[%s] Rejecting AppendEntries: leader term %d < current term %d",
			s.node.ID(), req.Term, currentTerm)

		return &pb.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}, nil
	}

	if req.Term >= currentTerm {
		if req.Term > currentTerm || currentState != Follower {
			s.node.BecomeFollower(req.Term)
		}
		s.node.ResetElectionTimer()
	}

	if !s.node.MatchesPrevLog(req.PrevLogIndex, req.PrevLogTerm) {
		log.Printf("[%s] Log mismatch at prevLogIndex=%d, prevLogTerm=%d",
			s.node.ID(), req.PrevLogIndex, req.PrevLogTerm)
		return &pb.AppendEntriesResponse{
			Term:    s.node.CurrentTerm(),
			Success: false,
		}, nil
	}

	if len(req.Entries) > 0 {
		s.node.AppendEntriesToLog(req.PrevLogIndex, req.Entries)
	}

	if req.LeaderCommit > s.node.CommitIndex() {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if lastNewIndex == 0 {
			lastNewIndex = s.node.LogLength()
		}
		newCommitIndex := min(lastNewIndex, req.LeaderCommit)
		if newCommitIndex > 0 {
			s.node.SetCommitIndex(newCommitIndex)
		}
	}

	response := &pb.AppendEntriesResponse{
		Term:    s.node.CurrentTerm(),
		Success: true,
	}

	if !isHeartbeat {
		log.Printf("[%s] AppendEntries success: appended %d entries", s.node.ID(), len(req.Entries))
	}

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
