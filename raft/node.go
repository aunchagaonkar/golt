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

type Node struct {
	mu sync.RWMutex

	id          string
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry
	kvStore     map[string]string
	wal         *WAL
	metaStore   *MetaStore

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

func NewNode(id, address string, peers []string, storageDir string) *Node {
	wal, err := OpenWAL(storageDir)
	if err != nil {
		log.Fatalf("[%s] Failed to open WAL: %v", id, err)
	}

	entries, err := wal.ReadAll()
	if err != nil {
		log.Fatalf("[%s] Failed to read WAL: %v", id, err)
	}
	log.Printf("[%s] Recovered %d entries from WAL", id, len(entries))

	metaStore := OpenMetaStore(storageDir)
	meta, err := metaStore.Load()
	if err != nil {
		log.Fatalf("[%s] Failed to load metadata: %v", id, err)
	}
	log.Printf("[%s] Loaded metadata: Term=%d, VotedFor=%s", id, meta.CurrentTerm, meta.VotedFor)

	return &Node{
		id:          id,
		address:     address,
		currentTerm: meta.CurrentTerm,
		votedFor:    meta.VotedFor,
		log:         entries,
		wal:         wal,
		metaStore:   metaStore,
		kvStore:     make(map[string]string),
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

func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) Address() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.address
}
func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}
func (n *Node) VotedFor() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.votedFor
}
func (n *Node) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}
func (n *Node) SetState(state NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
}
func (n *Node) SetVotedFor(candidateID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = candidateID
}
func (n *Node) SetCallbacks(onBecomeCandidate func(), onSendHeartbeat func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onBecomeCandidate = onBecomeCandidate
	n.onSendHeartbeat = onSendHeartbeat
}
func (n *Node) RandomElectionTimeout() time.Duration {
	diff := maxElectionTime - minElectionTime
	return minElectionTime + time.Duration(rand.Int63n(int64(diff)))
}
func (n *Node) StartElectionTimer() {
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
func (n *Node) ElectionTimerLoop() {
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
func (n *Node) HandleElectionTimeout() {
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
	n.saveMeta()

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

func (n *Node) ResetElectionTimer() {
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

func (n *Node) BecomeFollower(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	oldState := n.state
	oldTerm := n.currentTerm

	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.saveMeta()

	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}

	if oldState != Follower || oldTerm != term {
		log.Printf("[%s] Becoming Follower for term %d", n.id, term)
	}
}

func (n *Node) BecomeLeader() {
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

func (n *Node) StartHeartbeatLoop() {
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

func (n *Node) StopTimers() {
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

func (n *Node) LastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) LastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) IsLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
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

func (n *Node) CanGrantVote(candidateID string, candidateTerm, candidateLastLogIndex, candidateLastLogTerm uint64) (voteGranted bool, currTerm uint64) {
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
		n.saveMeta()
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
	n.saveMeta()
	log.Printf("[%s] Granting vote for %s in term %d", n.id, candidateID, n.currentTerm)
	return true, currTerm
}

func (n *Node) ClusterSize() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers) + 1
}
func (n *Node) MajoritySize() int {
	return (n.ClusterSize() / 2) + 1
}

func (n *Node) AppendCommand(cmd *pb.Command) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	newIndex := uint64(len(n.log) + 1)
	entry := &pb.LogEntry{
		Index:   newIndex,
		Term:    n.currentTerm,
		Command: cmd,
	}
	if err := n.wal.Append(entry); err != nil {
		log.Printf("[%s] WAL Append failed: %v", n.id, err)
		return 0
	}
	if err := n.wal.Sync(); err != nil {
		log.Printf("[%s] WAL Sync failed: %v", n.id, err)
		return 0
	}
	n.log = append(n.log, entry)
	log.Printf("[%s] Appended entry: index=%d, term=%d, cmd=%s %s", n.id, newIndex, n.currentTerm, cmd.Type, cmd.Key)
	return newIndex
}

func (n *Node) GetLogEntry(index uint64) *pb.LogEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if index == 0 || index > uint64(len(n.log)) {
		return nil
	}
	return n.log[index-1]
}

func (n *Node) GetLogEntries(startIndex uint64) []*pb.LogEntry {
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

func (n *Node) MatchesPrevLog(prevLogIndex, prevLogTerm uint64) bool {
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

func (n *Node) AppendEntriesToLog(prevLogIndex uint64, entries []*pb.LogEntry) bool {
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
				if err := n.wal.Truncate(uint64(insertIndex) + 1); err != nil {
					log.Printf("[%s] WAL Truncate failed: %v", n.id, err)
					return false
				}
				n.log = n.log[:insertIndex]
			} else {
				insertIndex++
				continue
			}
		}
		if err := n.wal.Append(entry); err != nil {
			log.Printf("[%s] WAL Append failed: %v", n.id, err)
			return false
		}
		n.log = append(n.log, entry)
		log.Printf("[%s] Appended entry from leader: index=%d, term=%d", n.id, entry.Index, entry.Term)
		insertIndex++
	}

	return true
}

func (n *Node) GetNextIndex(peerAddr string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if idx, ok := n.nextIndex[peerAddr]; ok {
		return idx
	}
	return uint64(len(n.log)) + 1
}

func (n *Node) SetNextIndex(peerAddr string, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextIndex[peerAddr] = index
}

func (n *Node) GetMatchIndex(peerAddr string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.matchIndex[peerAddr]
}

func (n *Node) SetMatchIndex(peerAddr string, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.matchIndex[peerAddr] = index
}

func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *Node) SetCommitIndex(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.commitIndex {
		log.Printf("[%s] Updated commitIndex: %d -> %d", n.id, n.commitIndex, index)
		n.commitIndex = index
		n.applyLog()
	}
}

func (n *Node) LogLength() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return uint64(len(n.log))
}

func (n *Node) GetPrevLogInfo(nextIndex uint64) (prevLogIndex, prevLogTerm uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	prevLogIndex = nextIndex - 1
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	return prevLogIndex, prevLogTerm
}

func (n *Node) UpdateCommitIndex() {
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
				n.applyLog()
			}
		}
	}
}

func (n *Node) applyLog() {
	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]

		if entry.Command == nil {
			continue
		}

		key := entry.Command.Key
		val := entry.Command.Value

		switch entry.Command.Type {
		case pb.CommandType_SET:
			n.kvStore[key] = val
			log.Printf("[%s] Applied: SET %s=%s (index=%d)", n.id, key, val, n.lastApplied)
		case pb.CommandType_DELETE:
			delete(n.kvStore, key)
			log.Printf("[%s] Applied: DELETE %s (index=%d)", n.id, key, n.lastApplied)
		}
	}
}

func (n *Node) GetValue(key string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	val, ok := n.kvStore[key]
	return val, ok
}

func (n *Node) saveMeta() {
	meta := &Metadata{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	}
	if err := n.metaStore.Save(meta); err != nil {
		log.Printf("[%s] Failed to save metadata: %v", n.id, err)
	}
}
