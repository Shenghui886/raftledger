package raft

import (
	"context"
	"sync"
	"sync/atomic"
)

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	latestBlk, _ := n.store.Latest()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.electionTimeout/2)
	defer cancel()

	req := RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: latestBlk.Index,
		LastLogTerm:  latestBlk.Term,
	}
	// RequestVote
	var vote atomic.Int64
	vote.Store(1)

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			resp, err := n.transport.RequestVote(ctx, p, req)
			if err == nil && resp.VoteGranted {
				vote.Add(1)
			}
		}(peer)
	}
	wg.Wait()
	// I'm leader
	if vote.Load() > int64((len(n.peers)+1)/2) {
		n.mu.Lock()
		n.state = Leader
		n.leaderID = n.id
		term := n.currentTerm
		leaderCommit := n.commitIndex

		blk, ok := n.store.Latest()
		var baseIdx uint64 = 0
		if ok {
			baseIdx = blk.Index + 1
		}

		for _, p := range n.peers {
			n.nextIndex[p] = baseIdx
			n.matchIndex[p] = 0
		}
		n.mu.Unlock()

		n.sendHeartbeat(term, leaderCommit)
		n.heartbeatTimer.Reset(n.heartbeatInterval)
	}
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	defer trySend(n.resetElectionTimerCh, struct{}{})

	if req.Term < n.currentTerm {
		return n.rejectVoteResp()
	}
	n.currentTerm = req.Term

	blk, _ := n.store.Latest()
	if (n.votedFor != -1 && n.votedFor != req.CandidateID) ||
		req.LastLogTerm < blk.Term ||
		(req.LastLogTerm == blk.Term && req.LastLogIndex < blk.Index) {
		return n.rejectVoteResp()
	}
	n.votedFor = req.CandidateID
	n.state = Follower
	return n.grantVoteResp()
}
