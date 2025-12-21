package raft

import (
	"log"

	pb "github.com/aunchagaonkar/golt/proto"
)

func (n *Node) HandleInstallSnapshot(
	term uint64,
	leaderID string,
	lastIncludedIndex uint64,
	lastIncludedTerm uint64,
	data map[string]string,
) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term < n.currentTerm {
		log.Printf("[%s] Rejecting InstallSnapshot from %s: term %d < currentTerm %d",
			n.id, leaderID, term, n.currentTerm)
		return n.currentTerm
	}

	n.resetElectionTimer()

	if term > n.currentTerm {
		log.Printf("[%s] InstallSnapshot from higher term %d, stepping down", n.id, term)
		n.currentTerm = term
		n.votedFor = ""
		n.state = Follower
		n.saveMeta()
	}

	if lastIncludedIndex <= n.lastIncludedIndex {
		log.Printf("[%s] Ignoring InstallSnapshot: lastIncludedIndex %d <= current %d",
			n.id, lastIncludedIndex, n.lastIncludedIndex)
		return n.currentTerm
	}

	log.Printf("[%s] Installing snapshot at index %d (term %d), %d keys",
		n.id, lastIncludedIndex, lastIncludedTerm, len(data))

	for k, v := range data {
		n.lsmStore.Put(k, v)
	}

	snap := &Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	if err := n.snapshotStore.SaveSnapshot(snap); err != nil {
		log.Printf("[%s] Failed to save installed snapshot: %v", n.id, err)
	}

	var keptLog []*pb.LogEntry
	for _, entry := range n.log {
		if entry.Index > lastIncludedIndex {
			keptLog = append(keptLog, entry)
		}
	}
	n.log = keptLog

	if err := n.wal.Compact(keptLog); err != nil {
		log.Printf("[%s] Failed to compact WAL after InstallSnapshot: %v", n.id, err)
	}

	n.lastIncludedIndex = lastIncludedIndex
	n.lastIncludedTerm = lastIncludedTerm

	if n.commitIndex < lastIncludedIndex {
		n.commitIndex = lastIncludedIndex
	}
	if n.lastApplied < lastIncludedIndex {
		n.lastApplied = lastIncludedIndex
	}

	log.Printf("[%s] Snapshot installed. lastIncludedIndex=%d, commitIndex=%d, log entries=%d",
		n.id, n.lastIncludedIndex, n.commitIndex, len(n.log))

	return n.currentTerm
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Reset(n.randomElectionTimeout())
	}
}
