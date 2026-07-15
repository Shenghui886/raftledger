package grpctransport

import (
	context "context"
	"net"

	"github.com/Shenghui886/raftledger/raft"
	"google.golang.org/grpc"
)

type raftServer struct {
	UnimplementedRaftServer
	node *raft.Node
}

func (s *raftServer) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return &RequestVoteResponse{}, err
	}
	return fromRaftVoteResp(s.node.HandleRequestVote(fromRPCVoteReq(req))), nil
}

func (s *raftServer) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return &AppendEntriesResponse{}, err
	}
	return fromRaftEntriesResp(s.node.HandleAppendEntries(fromRPCEntriesReq(req))), nil
}

func ListenAndServe(addr string, node *raft.Node) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	RegisterRaftServer(s, &raftServer{node: node})
	return s.Serve(ln)
}
