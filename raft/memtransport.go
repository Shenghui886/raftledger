package raft

import (
	"context"
	"fmt"
	"sync"
)

type MemoryTransport struct {
	mu    sync.RWMutex
	nodes map[int]*Node
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		nodes: make(map[int]*Node),
	}
}

func (mt *MemoryTransport) Register(node *Node) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.nodes[node.id] = node
}

func (mt *MemoryTransport) RequestVote(ctx context.Context, peer int, req RequestVoteRequest) (RequestVoteResponse, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return RequestVoteResponse{}, err
	}

	node, ok := mt.nodes[peer]
	if !ok {
		return RequestVoteResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	resp := node.HandleRequestVote(req)
	return resp, nil
}

func (mt *MemoryTransport) AppendEntries(ctx context.Context, peer int, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return AppendEntriesResponse{}, err
	}

	node, ok := mt.nodes[peer]
	if !ok {
		return AppendEntriesResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	resp := node.HandleAppendEntries(req)
	return resp, nil
}
