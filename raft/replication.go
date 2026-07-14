package raft

import (
	"context"

	"github.com/Shenghui886/raftledger/storage"
)

func (n *Node) sendAppendEntries(term, leaderCommit uint64) {
	n.mu.RLock()
	n.persisNow(term, n.votedFor)
	n.mu.RUnlock()

	for _, peer := range n.peers {
		n.mu.RLock()
		from := n.nextIndex[peer]
		n.mu.RUnlock()
		go func(p int, from uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), n.electionTimeout/2)
			defer cancel()

			req := n.buildAppendEntriesReq(from, term, leaderCommit)
			res, err := n.transport.AppendEntries(ctx, p, req)

			trySend(n.syncResultCh, syncResult{
				id:      p,
				count:   uint64(len(req.Entries)),
				success: err == nil && res.Success,
			})
		}(peer, from)
	}
}

func (n *Node) buildAppendEntriesReq(from, term, leaderCommit uint64) AppendEntriesRequest {
	var prevLogIndex, prevLogTerm uint64
	if from > 0 {
		prevLogIndex = from - 1
		if prevLogBlk, ok := n.store.Get(prevLogIndex); ok {
			prevLogTerm = prevLogBlk.Term
		}
	}

	var entries []storage.Block
	latestIdx := n.store.LatestIndex()
	if latestIdx > 0 && from > 0 && from <= latestIdx {
		for i := from; i <= latestIdx; i++ {
			blk, _ := n.store.Get(i)
			entries = append(entries, blk)
		}
	}

	return AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	defer func() { n.persisNow(req.Term, n.votedFor) }()

	if req.Term < n.currentTerm {
		return n.rejectResp(n.currentTerm)
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = -1
	}
	n.state = Follower
	n.leaderID = req.LeaderID
	trySend(n.resetElectionTimerCh, struct{}{})

	latestIdx := n.store.LatestIndex()
	if latestIdx > 0 {
		if req.PrevLogIndex > latestIdx {
			return n.rejectResp(n.currentTerm)
		}
		if req.PrevLogIndex > 0 {
			if blk, ok := n.store.Get(req.PrevLogIndex); !ok || req.PrevLogTerm != blk.Term {
				return n.rejectResp(n.currentTerm)
			}
		}
	} else if req.PrevLogIndex > 0 {
		return n.rejectResp(n.currentTerm)
	}

	if req.Entries == nil {
		return n.handleHeartbeat(req)
	}

	return n.handleLogReplication(req)
}

func (n *Node) handleHeartbeat(req AppendEntriesRequest) AppendEntriesResponse {
	if req.LeaderCommit > n.commitIndex {
		lastIdx := n.store.LatestIndex()
		n.setCommitIndex(min(req.LeaderCommit, lastIdx))
	}
	return n.successResp(n.currentTerm)
}

func (n *Node) handleLogReplication(req AppendEntriesRequest) AppendEntriesResponse {
	n.store.Truncate(req.PrevLogIndex + 1)
	for _, blk := range req.Entries {
		n.store.Append(blk)
	}
	if req.LeaderCommit > n.commitIndex {
		n.setCommitIndex(min(req.LeaderCommit, req.PrevLogIndex+uint64(len(req.Entries))))
	}
	return n.successResp(n.currentTerm)
}
