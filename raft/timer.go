package raft

import "time"

const (
	minElectionTime = 150 * time.Millisecond
	maxElectionTime = 300 * time.Millisecond
	heartbeatTime   = 50 * time.Millisecond
)
