package tcptransport

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/Shenghui886/raftledger/raft"
)

const MaxServeConn = 10

func ListenAndServe(addr string, node *raft.Node) error {
	var sem = make(chan struct{}, MaxServeConn)
	ln, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			time.Sleep(time.Millisecond)

			continue
		}
		sem <- struct{}{}
		go serveConn(conn, node, sem)
	}
}

func serveConn(conn net.Conn, node *raft.Node, sem <-chan struct{}) {
	defer func() { <-sem }()
	defer conn.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		env, err := readFrame(ctx, conn)
		if err != nil {
			cancel()
			return
		}

		switch env.Type {
		case TypeRequestVote:
			var req raft.RequestVoteRequest
			if err := json.Unmarshal(env.Body, &req); err != nil {
				cancel()
				return
			}
			resp := node.HandleRequestVote(req)
			rawResp, _ := json.Marshal(resp)
			if err := writeFrame(ctx, conn, envelope{Type: TypeRequestVoteResponse, Body: rawResp}); err != nil {
				cancel()
				return
			}

		case TypeAppendEntries:
			var req raft.AppendEntriesRequest
			if err := json.Unmarshal(env.Body, &req); err != nil {
				cancel()
				return
			}
			resp := node.HandleAppendEntries(req)
			rawResp, _ := json.Marshal(resp)
			if err := writeFrame(ctx, conn, envelope{Type: TypeAppendEntriesResponse, Body: rawResp}); err != nil {
				cancel()
				return
			}

		default:
			cancel()
			return
		}

		cancel()
	}
}
