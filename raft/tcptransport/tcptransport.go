package tcptransport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Shenghui886/raftledger/raft"
)

type TCPTransport struct {
	mu sync.RWMutex

	peerAddr map[int]string
	conns    map[int]*peerConn
}

type peerConn struct {
	mu   sync.Mutex
	conn net.Conn
}

type Envelope struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

const (
	TypeRequestVote           = "RequestVote"
	TypeAppendEntries         = "AppendEntries"
	TypeRequestVoteResponse   = "RequestVoteResponse"
	TypeAppendEntriesResponse = "AppendEntriesResponse"

	maxFrameSize = 10 * 1024 * 1024
)

func New(peerAddr map[int]string) *TCPTransport {
	return &TCPTransport{
		peerAddr: peerAddr,
		conns:    make(map[int]*peerConn),
	}
}

func (t *TCPTransport) getOrConnect(timeout time.Duration, peer int, addr string) (*peerConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	pc, ok := t.conns[peer]
	if ok && pc != nil {
		return pc, nil
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	t.conns[peer] = &peerConn{conn: conn}
	return t.conns[peer], nil
}

func (t *TCPTransport) evictConn(peer int) {
	t.mu.Lock()
	if pc := t.conns[peer]; pc != nil {
		pc.conn.Close()
		delete(t.conns, peer)
	}
	t.mu.Unlock()
}

func writeFrame(ctx context.Context, conn net.Conn, req Envelope) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

func readFrame(ctx context.Context, conn net.Conn) (Envelope, error) {
	deadline, _ := ctx.Deadline()
	conn.SetReadDeadline(deadline)

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return Envelope{}, err
	}

	length := binary.BigEndian.Uint32(header)
	if length > maxFrameSize {
		return Envelope{}, fmt.Errorf("frame too large: %d > %d", length, maxFrameSize)
	}
	data := make([]byte, length)

	if _, err := io.ReadFull(conn, data); err != nil {
		return Envelope{}, err
	}
	var resp Envelope
	err := json.Unmarshal(data, &resp)
	return resp, err
}

func (t *TCPTransport) RequestVote(ctx context.Context, peer int, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	addr, ok := t.peerAddr[peer]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("peer %d not found", peer)
	}

	dialTimeout := 3 * time.Second
	deadline, ok := ctx.Deadline()
	if ok {
		dialTimeout = time.Until(deadline)
	}
	pc, err := t.getOrConnect(dialTimeout, peer, addr)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	rawReq, err := json.Marshal(req)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	if err := writeFrame(ctx, pc.conn, Envelope{
		Type: TypeRequestVote,
		Body: rawReq,
	}); err != nil {
		t.evictConn(peer)
		return raft.RequestVoteResponse{}, err
	}

	env, err := readFrame(ctx, pc.conn)
	if err != nil {
		t.evictConn(peer)
		return raft.RequestVoteResponse{}, err
	}

	var resp raft.RequestVoteResponse
	if err := json.Unmarshal(env.Body, &resp); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return resp, nil
}

func (t *TCPTransport) AppendEntries(ctx context.Context, peer int, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	addr, ok := t.peerAddr[peer]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("peer %d not found", peer)
	}
	dialTimeout := 3 * time.Second
	deadline, ok := ctx.Deadline()
	if ok {
		dialTimeout = time.Until(deadline)
	}
	pc, err := t.getOrConnect(dialTimeout, peer, addr)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	rawReq, err := json.Marshal(req)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := writeFrame(ctx, pc.conn, Envelope{
		Type: TypeAppendEntries,
		Body: rawReq,
	}); err != nil {
		t.evictConn(peer)
		return raft.AppendEntriesResponse{}, err
	}

	env, err := readFrame(ctx, pc.conn)
	if err != nil {
		t.evictConn(peer)
		return raft.AppendEntriesResponse{}, err
	}

	var resp raft.AppendEntriesResponse
	if err := json.Unmarshal(env.Body, &resp); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return resp, nil
}
