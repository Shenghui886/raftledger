package raft

import "github.com/Shenghui886/raftledger/storage"

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	Entries      []storage.Block
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}
