package raft

import (
	"log"

	pb "github.com/aunchagaonkar/golt/proto"
)

func (n *Node) takeSnapshot() {
	snapshotIndex := n.lastApplied
	if snapshotIndex <= n.lastIncludedIndex {
		return
	}
	snapshotTerm := n.getLogTerm(snapshotIndex)
	log.Printf("[%s] Taking snapshot at index %d (term %d)", n.id, snapshotIndex, snapshotTerm)

	data, err := n.lsmStore.GetAll()
	if err != nil {
		log.Printf("[%s] Failed to get state for snapshot: %v", n.id, err)
		return
	}

	snap := &Snapshot{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  snapshotTerm,
		Data:              data,
	}

	if err := n.snapshotStore.SaveSnapshot(snap); err != nil {
		log.Printf("[%s] Failed to save snapshot: %v", n.id, err)
		return
	}

	var keepEntries []*pb.LogEntry
	for _, entry := range n.log {
		if entry.Index > snapshotIndex {
			keepEntries = append(keepEntries, entry)
		}
	}

	if err := n.wal.Compact(keepEntries); err != nil {
		log.Printf("[%s] Failed to compact WAL: %v", n.id, err)
		return
	}

	n.lastIncludedIndex = snapshotIndex
	n.lastIncludedTerm = snapshotTerm
	n.log = keepEntries

	log.Printf("[%s] Snapshot complete. Log truncated. Base Index: %d, Logs in Memory: %d",
		n.id, n.lastIncludedIndex, len(n.log))
}

func (n *Node) checkSnapshot() {
	if len(n.log) > 1000 {
		n.takeSnapshot()
	}
}
