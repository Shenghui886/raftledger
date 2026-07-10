package raft

import (
	"context"

	"github.com/Shenghui886/raftledger/storage"
)

func (n *Node) sendHeartbeat(term uint64) {
	for _, peer := range n.peers {
		from := n.nextIndex[peer]
		go func(p int, from uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), n.electionTimeout/2)
			defer cancel()

			var prevLogIndex uint64
			if from > 0 {
				prevLogIndex = from - 1
			}
			var entries []storage.Block
			latestBlk, ok := n.store.Latest()
			if ok && from <= latestBlk.Index {
				for i := from; i <= latestBlk.Index; i++ {
					blk, _ := n.store.Get(i)
					entries = append(entries, blk)
				}
			}

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				Entries:      entries,
				LeaderCommit: 0,
			}
			res, err := n.transport.AppendEntries(ctx, p, req)
			var count uint64 = 0
			if entries != nil {
				if err == nil && res.Success {
					count = uint64(len(entries))
				}
				select {
				case n.syncResultCh <- syncResult{id: p, count: count}:
				default:
				}
			}
		}(peer, from)
	}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	latestBlk, _ := n.store.Latest()
	if req.Term < n.currentTerm ||
		(req.Entries != nil && req.PrevLogIndex > latestBlk.Index) {
		return AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = -1
	}
	n.state = Follower
	n.leaderID = req.LeaderID

	for _, blk := range req.Entries {
		if err := n.store.Append(blk); err != nil {
			return AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}
		}
	}
	select {
	case n.resetElectionTimerCh <- struct{}{}:
	default:
	}
	return AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}
}
