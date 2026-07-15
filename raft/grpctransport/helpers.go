package grpctransport

import (
	"github.com/Shenghui886/raftledger/raft"
	"github.com/Shenghui886/raftledger/storage"
)

func fromRaftVoteReq(req raft.RequestVoteRequest) *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  int32(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
}

func fromRaftVoteResp(resp raft.RequestVoteResponse) *RequestVoteResponse {
	return &RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}
}

func fromRPCVoteReq(req *RequestVoteRequest) raft.RequestVoteRequest {
	return raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  int(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
}

func fromRPCVoteResp(resp *RequestVoteResponse) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}
}

func fromRaftEntriesReq(req raft.AppendEntriesRequest) *AppendEntriesRequest {
	var entries []*Block = make([]*Block, 0, len(req.Entries))
	for _, blk := range req.Entries {
		entries = append(entries, &Block{
			Index:     blk.Index,
			Term:      blk.Term,
			Timestamp: blk.Timestamp,
			Data:      &Transaction{Data: blk.Data.Data},
		})
	}
	return &AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     int32(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
}

func fromRaftEntriesResp(resp raft.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}
}

func fromRPCEntriesReq(req *AppendEntriesRequest) raft.AppendEntriesRequest {
	var entries []storage.Block = make([]storage.Block, 0, len(req.Entries))
	for _, blk := range req.Entries {
		entries = append(entries, storage.Block{
			Index:     blk.Index,
			Term:      blk.Term,
			Timestamp: blk.Timestamp,
			Data:      storage.Transaction{Data: blk.Data.Data},
		})
	}
	return raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
}

func fromRPCEntriesResp(resp *AppendEntriesResponse) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}
}
