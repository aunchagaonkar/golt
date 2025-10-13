package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
)

type NodeState int // role of node in raft

const (
	Follower  NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

func (state NodeState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (state NodeState) ToProto() pb.NodeState {
	switch state {
	case Follower:
		return pb.NodeState_FOLLOWER
	case Candidate:
		return pb.NodeState_CANDIDATE
	case Leader:
		return pb.NodeState_LEADER
	default:
		return pb.NodeState_FOLLOWER
	}
}

type node struct {
	mu sync.RWMutex

	id          string
	currentTerm uint64
	votedFor    string
	log         []pb.LogEntry

	state       NodeState
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	peers   []string
	address string

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	stopCh          chan struct{}
	running         bool

	onBecomeCandidate func()
	onSendHeartbeat   func()
}

func NewNode(id, address string, peers []string) *node {
	return &node{
		id:          id,
		address:     address,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]pb.LogEntry, 0),
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		peers:       peers,
		stopCh:      make(chan struct{}),
		running:     false,
	}
}

func (n *node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *node) Address() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.address
}
func (n *node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}
func (n *node) VotedFor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.votedFor
}
func (n *node) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}
func (n *node) SetState(state NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
}
func (n *node) SetVotedFor(candidateID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = candidateID
}
func (n *node) SetCallbacks(onBecomeCandidate func(), onSendHeartbeat func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onBecomeCandidate = onBecomeCandidate
	n.onSendHeartbeat = onSendHeartbeat
}
func (n *node) RandomElectionTimeout() time.Duration {
	diff := maxElectionTime - minElectionTime
	return minElectionTime + time.Duration(rand.Int63n(int64(diff)))
}
func (n *node) StartElectionTimer() {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return
	}
	n.running = true
	n.stopCh = make(chan struct{})
	timeout := n.RandomElectionTimeout()
	n.electionTimer = time.NewTimer(timeout)
	n.mu.Unlock()

	log.Printf("[%s] Starting election timer (timeout: %v)", n.id, timeout)
	go n.ElectionTimerLoop()
}
func (n *node) ElectionTimerLoop() {
	for {
		n.mu.RLock()
		timer := n.electionTimer
		stopCh := n.stopCh
		n.mu.RUnlock()

		select {
		case <-stopCh:
			log.Printf("[%s] Election timer stopped", n.ID())
			return
		case <-timer.C:
			n.HandleElectionTimeout()
		}
	}
}
func (n *node) HandleElectionTimeout() {
	n.mu.Lock()

	if n.state == Leader {
		timeout := n.RandomElectionTimeout()
		n.electionTimer.Reset(timeout)
		n.mu.Unlock()
		return
	}
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id

	term := n.currentTerm
	id := n.id
	callback := n.onBecomeCandidate

	timeout := n.RandomElectionTimeout()
	n.electionTimer.Reset(timeout)
	n.mu.Unlock()

	log.Printf("[%s] Election timeout expired! Becoming Candidate for term %d", id, term)
	if callback != nil {
		callback()
	}
}

func (n *node) ResetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.electionTimer == nil {
		return
	}
	timeout := n.RandomElectionTimeout()

	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(timeout)
	log.Printf("[%s] Election timer reset (new timeout: %v)", n.id, timeout)
}

func (n *node) BecomeFollower(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	oldState := n.state
	oldTerm := n.currentTerm

	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""

	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}

	if oldState != Follower || oldTerm != term {
		log.Printf("[%s] Becoming Follower for term %d", n.id, term)
	}
}

func (n *node) BecomeLeader() {
	n.mu.Lock()

	if n.state == Leader {
		return
	}

	n.state = Leader
	term := n.currentTerm
	id := n.id

	for _, peer := range n.peers {
		n.nextIndex[peer] = uint64(len(n.log))
		n.matchIndex[peer] = 0
	}
	n.mu.Unlock()

	log.Printf("[%s] Becoming Leader for term %d", id, term)
	n.StartHeartbeatLoop()
}

func (n *node) StartHeartbeatLoop() {
	n.mu.Lock()
	n.heartbeatTicker = time.NewTicker(heartbeatTime)
	ticker := n.heartbeatTicker
	stopCh := n.stopCh
	callback := n.onSendHeartbeat
	n.mu.Unlock()
	log.Printf("[%s] Starting heartbeat loop - interval: %v", n.ID(), heartbeatTime)
	go func() {
		if callback != nil {
			callback()
		}
		for {
			select {
			case <-stopCh:
				log.Printf("[%s] Heartbeat loop stopped", n.ID())
				return
			case <-ticker.C:
				n.mu.RLock()
				state := n.state
				n.mu.RUnlock()
				if state != Leader {
					log.Printf("[%s] No longer leader, stopping heartbeat loop", n.ID())
					return
				}
				if callback != nil {
					callback()
				}
			}
		}
	}()
}

func (n *node) StopTimers() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return
	}
	n.running = false
	close(n.stopCh)
	if n.electionTimer != nil {
		n.electionTimer.Stop()
		n.electionTimer = nil
	}

	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}

	log.Printf("[%s] All timers stopped", n.id)
}
