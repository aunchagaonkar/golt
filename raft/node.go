package raft

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/aunchagaonkar/golt/proto"
	"github.com/aunchagaonkar/golt/storage"
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

	id            string
	currentTerm   uint64
	votedFor      string
	log           []*pb.LogEntry
	wal           *WAL
	metaStore     *MetaStore
	lsmStore      *storage.LSMStore
	snapshotStore *SnapshotStore

	state             NodeState
	commitIndex       uint64
	lastApplied       uint64
	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	peers      []string
	address    string
	leaderAddr string

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

	lsmStore, err := storage.OpenLSMStore(storageDir, 1024*1024) // 1MB MemTable
	if err != nil {
		log.Fatalf("[%s] Failed to open LSM store: %v", id, err)
	}

	snapshotStore := OpenSnapshotStore(storageDir)
	snap, err := snapshotStore.LoadSnapshot()
	if err != nil {
		log.Fatalf("[%s] Failed to load snapshot: %v", id, err)
	}

	var lastIncludedIndex, lastIncludedTerm uint64
	if snap != nil {
		lastIncludedIndex = snap.LastIncludedIndex
		lastIncludedTerm = snap.LastIncludedTerm
		log.Printf("[%s] Loaded Snapshot (Index: %d, Term: %d)", id, lastIncludedIndex, lastIncludedTerm)
		for k, v := range snap.Data {
			lsmStore.Put(k, v)
		}
	}

	var filteredLog []*pb.LogEntry
	for _, entry := range entries {
		if entry.Index > lastIncludedIndex {
			filteredLog = append(filteredLog, entry)
		}
	}
	log.Printf("[%s] Filtered log: %d entries remaining after snapshot (Index %d)", id, len(filteredLog), lastIncludedIndex)

	recoveredCommitIndex := max(meta.CommitIndex, lastIncludedIndex)
	recoveredLastApplied := max(meta.LastApplied, lastIncludedIndex)

	log.Printf("[%s] Recovery: commitIndex=%d, lastApplied=%d (from metadata)", id, recoveredCommitIndex, recoveredLastApplied)

	node := &Node{
		id:                id,
		address:           address,
		currentTerm:       meta.CurrentTerm,
		votedFor:          meta.VotedFor,
		log:               filteredLog,
		wal:               wal,
		metaStore:         metaStore,
		lsmStore:          lsmStore,
		snapshotStore:     snapshotStore,
		state:             Follower,
		commitIndex:       recoveredCommitIndex,
		lastApplied:       recoveredLastApplied,
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		peers:             peers,
		stopCh:            make(chan struct{}),
		running:           false,
	}

	node.recoverCommittedEntries()

	return node
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
	peersCopy := make([]string, len(n.peers))
	copy(peersCopy, n.peers)
	return peersCopy
}

func (n *Node) LeaderAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderAddr
}

func (n *Node) SetLeaderAddr(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderAddr = addr
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

func (n *Node) randomElectionTimeout() time.Duration {
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
	timeout := n.randomElectionTimeout()
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
		timeout := n.randomElectionTimeout()
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

	timeout := n.randomElectionTimeout()
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
	timeout := n.randomElectionTimeout()

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
		log.Printf("[%s] Becoming Follower (term: %d, was: %s term %d)", n.id, term, oldState, oldTerm)
	}
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()

	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Leader
	n.leaderAddr = n.id
	term := n.currentTerm
	id := n.id
	lastLogIndex := n.getLastLogIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
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

func (n *Node) getLastLogIndex() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Index
	}
	return n.lastIncludedIndex
}

func (n *Node) getLastLogTerm() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Term
	}
	return n.lastIncludedTerm
}

func (n *Node) getLogEntry(index uint64) *pb.LogEntry {
	if index <= n.lastIncludedIndex {
		return nil
	}
	offset := index - n.lastIncludedIndex - 1
	if offset >= uint64(len(n.log)) {
		return nil
	}
	return n.log[offset]
}

func (n *Node) getLogTerm(index uint64) uint64 {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	if index < n.lastIncludedIndex {
		return 0
	}
	offset := index - n.lastIncludedIndex - 1
	if offset >= uint64(len(n.log)) {
		return 0
	}
	return n.log[offset].Term
}

func (n *Node) LastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.getLastLogIndex()
}

func (n *Node) LastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.getLastLogTerm()
}

func (n *Node) IsLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	myLastLogTerm := n.getLastLogTerm()
	myLastLogIndex := n.getLastLogIndex()

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

	myLastLogTerm := n.getLastLogTerm()
	myLastLogIndex := n.getLastLogIndex()

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
	// defer n.mu.Unlock()

	newIndex := n.getLastLogIndex() + 1
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
	n.mu.Unlock()

	n.UpdateCommitIndex()
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
	if startIndex <= n.lastIncludedIndex {
		startIndex = n.lastIncludedIndex + 1
	}
	offset := startIndex - n.lastIncludedIndex - 1
	if offset >= uint64(len(n.log)) {
		return []*pb.LogEntry{}
	}
	return n.log[offset:]
}

func (n *Node) MatchesPrevLog(prevLogIndex, prevLogTerm uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if prevLogIndex == 0 {
		return true
	}
	if prevLogIndex > n.getLastLogIndex() {
		return false
	}

	term := n.getLogTerm(prevLogIndex)
	if term == 0 {
		return false
	}

	return term == prevLogTerm
}

func (n *Node) AppendEntriesToLog(prevLogIndex uint64, entries []*pb.LogEntry) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(entries) == 0 {
		return true
	}

	for _, entry := range entries {
		if entry.Index < n.lastIncludedIndex {
			continue
		}
		offset := int(entry.Index - n.lastIncludedIndex - 1)

		if offset < len(n.log) {
			if n.log[offset].Term != entry.Term {
				log.Printf("[%s] Log conflict at index %d: deleting entries from index %d onwards", n.id, entry.Index, entry.Index)

				if err := n.wal.Truncate(entry.Index); err != nil {
					log.Printf("[%s] WAL Truncate failed: %v", n.id, err)
					return false
				}
				n.log = n.log[:offset]
			} else {
				continue
			}
		}
		if err := n.wal.Append(entry); err != nil {
			log.Printf("[%s] WAL Append failed: %v", n.id, err)
			return false
		}
		n.log = append(n.log, entry)
		log.Printf("[%s] Appended entry from leader: index=%d, term=%d", n.id, entry.Index, entry.Term)
	}

	if err := n.wal.Sync(); err != nil {
		log.Printf("[%s] WAL Sync failed: %v", n.id, err)
		return false
	}

	return true
}

func (n *Node) GetNextIndex(peerAddr string) uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if idx, ok := n.nextIndex[peerAddr]; ok {
		return idx
	}
	// default to 1 for unknown peers - they likely need everything
	// this will trigger InstallSnapshot if we have a snapshot
	return 1
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
		n.saveMeta()
	}
}

func (n *Node) LogLength() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.getLastLogIndex()
}

func (n *Node) LastIncludedIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastIncludedIndex
}

func (n *Node) LastIncludedTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastIncludedTerm
}

func (n *Node) LoadSnapshot() (*Snapshot, error) {
	return n.snapshotStore.LoadSnapshot()
}

func (n *Node) GetPrevLogInfo(nextIndex uint64) (prevLogIndex, prevLogTerm uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	prevLogIndex = nextIndex - 1
	prevLogTerm = n.getLogTerm(prevLogIndex)
	return prevLogIndex, prevLogTerm
}

func (n *Node) UpdateCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}

	matchIndices := make([]uint64, 0, len(n.peers)+1)
	matchIndices = append(matchIndices, n.getLastLogIndex())
	for _, idx := range n.matchIndex {
		matchIndices = append(matchIndices, idx)
	}

	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] > matchIndices[j]
	})

	majorityIdx := len(n.peers) / 2
	N := matchIndices[majorityIdx]

	if N > n.commitIndex {
		term := n.getLogTerm(N)
		if term == n.currentTerm {
			log.Printf("[%s] CommitIndex updated: %d -> %d (majority reached)",
				n.id, n.commitIndex, N)
			n.commitIndex = N
			n.applyLog()
			n.saveMeta()
		}
	}
}

func (n *Node) applyLog() {
	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		entry := n.getLogEntry(n.lastApplied)
		if entry == nil {
			break
		}
		if entry.Command == nil {
			continue
		}

		key := entry.Command.Key
		val := entry.Command.Value

		switch entry.Command.Type {
		case pb.CommandType_SET:
			if err := n.lsmStore.Put(key, val); err != nil {
				log.Printf("[%s] Failed to apply SET: %v", n.id, err)
			}
			log.Printf("[%s] Applied: SET %s=%s (index=%d)", n.id, key, val, n.lastApplied)
		case pb.CommandType_DELETE:
			if err := n.lsmStore.Delete(key); err != nil {
				log.Printf("[%s] Failed to apply DELETE: %v", n.id, err)
			}
			log.Printf("[%s] Applied: DELETE %s (index=%d)", n.id, key, n.lastApplied)
		}
	}
	n.checkSnapshot()
}

func (n *Node) GetValue(key string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lsmStore.Get(key)
}

func (n *Node) saveMeta() {
	meta := &Metadata{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
	if err := n.metaStore.Save(meta); err != nil {
		log.Printf("[%s] Failed to save metadata: %v", n.id, err)
	}
}

func (n *Node) recoverCommittedEntries() {
	replayCount := 0
	for i := n.lastIncludedIndex + 1; i <= n.lastApplied; i++ {
		entry := n.getLogEntry(i)
		if entry == nil || entry.Command == nil {
			continue
		}
		key := entry.Command.Key
		val := entry.Command.Value

		switch entry.Command.Type {
		case pb.CommandType_SET:
			if err := n.lsmStore.Put(key, val); err != nil {
				log.Printf("[%s] Failed to replay SET: %v", n.id, err)
			}
			replayCount++
		case pb.CommandType_DELETE:
			if err := n.lsmStore.Delete(key); err != nil {
				log.Printf("[%s] Failed to replay DELETE: %v", n.id, err)
			}
			replayCount++
		}
	}
	if replayCount > 0 {
		log.Printf("[%s] Replayed %d entries to state machine", n.id, replayCount)
	}
}
