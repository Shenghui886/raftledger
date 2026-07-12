package raft

import (
	"context"
	"sync"
	"sync/atomic"
)

func (n *Node) prepElection() (term uint64, lastIdx uint64, lastTerm uint64) {
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	if blk, ok := n.store.Get(n.store.LatestIndex()); ok {
		return n.currentTerm, blk.Index, blk.Term
	}
	return n.currentTerm, 0, 0
}

func (n *Node) winElection() (term uint64, leaderCommit uint64) {
	n.state = Leader
	n.leaderID = n.id
	for _, p := range n.peers {
		n.nextIndex[p] = n.store.LatestIndex() + 1
		n.matchIndex[p] = 0
	}
	return n.currentTerm, n.commitIndex
}

func (n *Node) startElection() {
	n.mu.Lock()
	term, lastIdx, lastTerm := n.prepElection()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.electionTimeout/2)
	defer cancel()

	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

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

	if vote.Load() > int64((len(n.peers)+1)/2) {
		n.mu.Lock()
		term, leaderCommit := n.winElection()
		n.mu.Unlock()

		n.sendAppendEntries(term, leaderCommit)
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

	blk, _ := n.store.Get(n.store.LatestIndex())
	if (n.votedFor != -1 && n.votedFor != req.CandidateID) ||
		req.LastLogTerm < blk.Term ||
		(req.LastLogTerm == blk.Term && req.LastLogIndex < blk.Index) {
		return n.rejectVoteResp()
	}
	n.votedFor = req.CandidateID
	n.state = Follower
	return n.grantVoteResp()
}
