package raft

import (
	"log"
	"math/rand"
	"sort"
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
	log         []*pb.LogEntry

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
		log:         make([]*pb.LogEntry, 0),
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
		n.nextIndex[peer] = uint64(len(n.log)) + 1
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

func (n *node) LastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *node) LastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *node) IsLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var myLastLogTerm, myLastLogIndex uint64
	if len(n.log) > 0 {
		myLastLogTerm = n.log[len(n.log)-1].Term
		myLastLogIndex = n.log[len(n.log)-1].Index
	}

	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIndex >= myLastLogIndex
}

func (n *node) CanGrantVote(candidateID string, candidateTerm, candidateLastLogIndex, candidateLastLogTerm uint64) (voteGranted bool, currTerm uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	currTerm = n.currentTerm

	if candidateTerm < n.currentTerm {
		log.Printf("[%s] Rejecting vote for %s: candidate term %d < current term %d", n.id, candidateID, candidateTerm, n.currentTerm)
		return false, currTerm
	}

	if candidateTerm > n.currentTerm {
		log.Printf("[%s] Updating term from %d to %d from RequestVote", n.id, n.currentTerm, candidateTerm)
		n.currentTerm = candidateTerm
		n.votedFor = ""
		n.state = Follower
		currTerm = n.currentTerm
	}

	canVote := (n.votedFor == "" || n.votedFor == candidateID)
	if !canVote {
		log.Printf("[%s] Rejecting vote for %s: already voted for %s in term %d", n.id, candidateID, n.votedFor, n.currentTerm)
		return false, currTerm
	}

	var myLastLogTerm, myLastLogIndex uint64
	if len(n.log) > 0 {
		myLastLogTerm = n.log[len(n.log)-1].Term
		myLastLogIndex = n.log[len(n.log)-1].Index
	}

	isUpToDate := false
	if candidateLastLogTerm != myLastLogTerm {
		isUpToDate = candidateLastLogTerm > myLastLogTerm
	} else {
		isUpToDate = candidateLastLogIndex >= myLastLogIndex
	}

	if !isUpToDate {
		log.Printf("[%s] Rejecting vote for %s: candidate's log is not up-to-date", n.id, candidateID)
		return false, currTerm
	}

	n.votedFor = candidateID
	log.Printf("[%s] Granting vote for %s in term %d", n.id, candidateID, n.currentTerm)
	return true, currTerm
}

func (n *node) ClusterSize() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers) + 1
}
func (n *node) MajoritySize() int {
	return (n.ClusterSize() / 2) + 1
}

func (n *node) AppendCommand(cmd *pb.Command) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	newIndex := uint64(len(n.log) + 1)
	entry := &pb.LogEntry{
		Index:   newIndex,
		Term:    n.currentTerm,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	log.Printf("[%s] Appended entry: index=%d, term=%d, cmd=%s %s", n.id, newIndex, n.currentTerm, cmd.Type, cmd.Key)
	return newIndex
}

func (n *node) GetLogEntry(index uint64) *pb.LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if index == 0 || index > uint64(len(n.log)) {
		return nil
	}
	return n.log[index-1]
}

func (n *node) GetLogEntries(startIndex uint64) []*pb.LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if startIndex == 0 {
		startIndex = 1
	}
	if startIndex > uint64(len(n.log)) {
		return []*pb.LogEntry{}
	}
	return n.log[startIndex-1:]
}

func (n *node) MatchesPrevLog(prevLogIndex, prevLogTerm uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if prevLogIndex == 0 {
		return true
	}
	if prevLogIndex > uint64(len(n.log)) {
		return false
	}
	return n.log[prevLogIndex-1].Term == prevLogTerm
}

func (n *node) AppendEntriesToLog(prevLogIndex uint64, entries []*pb.LogEntry) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(entries) == 0 {
		return true
	}

	insertIndex := int(prevLogIndex)

	for _, entry := range entries {
		if insertIndex < len(n.log) {
			if n.log[insertIndex].Term != entry.Term {
				log.Printf("[%s] Log conflict at index %d: deleting entries from index %d onwards", n.id, entry.Index, insertIndex+1)
				n.log = n.log[:insertIndex]
			} else {
				insertIndex++
				continue
			}
		}
		n.log = append(n.log, entry)
		log.Printf("[%s] Appended entry from leader: index=%d, term=%d", n.id, entry.Index, entry.Term)
		insertIndex++
	}

	return true
}

func (n *node) GetNextIndex(peerAddr string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if idx, ok := n.nextIndex[peerAddr]; ok {
		return idx
	}
	return uint64(len(n.log)) + 1
}

func (n *node) SetNextIndex(peerAddr string, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextIndex[peerAddr] = index
}

func (n *node) GetMatchIndex(peerAddr string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.matchIndex[peerAddr]
}

func (n *node) SetMatchIndex(peerAddr string, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.matchIndex[peerAddr] = index
}

func (n *node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *node) SetCommitIndex(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.commitIndex {
		log.Printf("[%s] Updated commitIndex: %d -> %d", n.id, n.commitIndex, index)
		n.commitIndex = index
	}
}

func (n *node) LogLength() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return uint64(len(n.log))
}

func (n *node) GetPrevLogInfo(nextIndex uint64) (prevLogIndex, prevLogTerm uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	prevLogIndex = nextIndex - 1
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	return prevLogIndex, prevLogTerm
}

func (n *node) UpdateCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}

	matchIndices := make([]uint64, 0, len(n.peers)+1)
	matchIndices = append(matchIndices, uint64(len(n.log)))
	for _, idx := range n.matchIndex {
		matchIndices = append(matchIndices, idx)
	}

	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] > matchIndices[j]
	})

	majorityIdx := len(n.peers) / 2
	N := matchIndices[majorityIdx]

	if N > n.commitIndex {
		if N > 0 && N <= uint64(len(n.log)) {
			if n.log[N-1].Term == n.currentTerm {
				log.Printf("[%s] CommitIndex updated: %d -> %d (majority reached)",
					n.id, n.commitIndex, N)
				n.commitIndex = N
			}
		}
	}
}
