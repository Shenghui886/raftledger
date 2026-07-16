package memtransport

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shenghui886/raftledger/raft"
)

type memoryTransport struct {
	mu    sync.RWMutex
	nodes map[int]*raft.Node
}

func New(nodes map[int]*raft.Node) raft.Transporter {
	return &memoryTransport{
		nodes: nodes,
	}
}

func (mt *memoryTransport) RequestVote(ctx context.Context, peer int, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return raft.RequestVoteResponse{}, err
	}

	node, ok := mt.nodes[peer]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	resp := node.HandleRequestVote(req)
	return resp, nil
}

func (mt *memoryTransport) AppendEntries(ctx context.Context, peer int, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	node, ok := mt.nodes[peer]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	resp := node.HandleAppendEntries(req)
	return resp, nil
}
