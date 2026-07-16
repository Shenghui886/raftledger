package grpctransport

import (
	context "context"
	"fmt"
	sync "sync"
	"time"

	"github.com/Shenghui886/raftledger/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type gRPCTransport struct {
	mu sync.RWMutex

	peerAddr map[int]string
	conns    map[int]*grpc.ClientConn
}

func New(peerAddr map[int]string) raft.Transporter {
	return &gRPCTransport{
		peerAddr: peerAddr,
		conns:    make(map[int]*grpc.ClientConn),
	}
}

func (t *gRPCTransport) getOrConnect(timeout time.Duration, peer int, addr string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	pc, ok := t.conns[peer]
	if ok && pc != nil {
		return pc, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: timeout}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	t.conns[peer] = conn

	return t.conns[peer], nil
}

func (t *gRPCTransport) RequestVote(ctx context.Context, peer int, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	addr, ok := t.peerAddr[peer]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	var timeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}
	conn, err := t.getOrConnect(timeout, peer, addr)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	client := NewRaftClient(conn)
	res, err := client.RequestVote(ctx, fromRaftVoteReq(req))
	return fromRPCVoteResp(res), err
}

func (t *gRPCTransport) AppendEntries(ctx context.Context, peer int, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	addr, ok := t.peerAddr[peer]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	var timeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}
	conn, err := t.getOrConnect(timeout, peer, addr)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	client := NewRaftClient(conn)
	res, err := client.AppendEntries(ctx, fromRaftEntriesReq(req))
	return fromRPCEntriesResp(res), err
}
