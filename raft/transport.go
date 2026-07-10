package raft

import "context"

type Transport interface {
	RequestVote(ctx context.Context, peer int, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer int, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
