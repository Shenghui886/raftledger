package raft

import (
	"context"

	"github.com/Shenghui886/raftledger/storage"
)

func (n *Node) sendHeartbeat(term, leaderCommit uint64) {
	for _, peer := range n.peers {
		from := n.nextIndex[peer]
		go func(p int, from uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), n.electionTimeout/2)
			defer cancel()

			req := n.buildAppendEntriesReq(from, term, leaderCommit)
			res, err := n.transport.AppendEntries(ctx, p, req)
			var count uint64 = 0
			if req.Entries != nil {
				if err == nil && res.Success {
					count = uint64(len(req.Entries))
				}
				trySend(n.syncResultCh, syncResult{id: p, count: count})
			}
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
	latestBlk, ok := n.store.Latest()
	if ok && from <= latestBlk.Index {
		for i := from; i <= latestBlk.Index; i++ {
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

	if req.Term < n.currentTerm {
		return n.rejectResp()
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = -1
	}
	n.state = Follower
	n.leaderID = req.LeaderID
	trySend(n.resetElectionTimerCh, struct{}{})

	if req.Entries == nil {
		return n.handleHeartbeat(req)
	}
	return n.handleLogReplication(req)
}

func (n *Node) handleHeartbeat(req AppendEntriesRequest) AppendEntriesResponse {
	if req.LeaderCommit > n.commitIndex {
		var lastIdx uint64
		if latestBlk, ok := n.store.Latest(); ok {
			lastIdx = latestBlk.Index
		}
		n.setCommitIndex(min(req.LeaderCommit, lastIdx))
	}
	return n.successResp()
}

func (n *Node) handleLogReplication(req AppendEntriesRequest) AppendEntriesResponse {
	latestBlk, notEmpty := n.store.Latest()
	if notEmpty {
		if req.PrevLogIndex > latestBlk.Index {
			return n.rejectResp()
		}
		if blk, ok := n.store.Get(req.PrevLogIndex); !ok || req.PrevLogTerm != blk.Term {
			return n.rejectResp()
		}
	} else if req.PrevLogIndex > 0 {
		return n.rejectResp()
	}

	n.store.Truncate(req.PrevLogIndex + 1)
	for _, blk := range req.Entries {
		n.store.Append(blk)
	}
	if req.LeaderCommit > n.commitIndex {
		n.setCommitIndex(min(req.LeaderCommit, req.PrevLogIndex+uint64(len(req.Entries))))
	}
	return n.successResp()
}
