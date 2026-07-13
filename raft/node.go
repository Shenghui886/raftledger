package raft

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/Shenghui886/raftledger/storage"
)

type Node struct {
	mu sync.RWMutex

	currentTerm uint64

	votedFor int
	leaderID int

	store     *storage.LedgerStore
	persister storage.Persister

	state NodeState
	id    int

	transport Transporter

	electionTimeout   time.Duration
	heartbeatInterval time.Duration

	peers        []int
	nextIndex    map[int]uint64
	syncResultCh chan syncResult
	commitIndex  uint64
	matchIndex   map[int]uint64
	lastApplied  uint64
	applyCh      chan struct{}

	electionTimer        *time.Timer
	heartbeatTimer       *time.Timer
	resetElectionTimerCh chan struct{}
}

type syncResult struct {
	id      int
	count   uint64
	success bool
}

func NewNode(id int, store *storage.LedgerStore, persister storage.Persister, transport Transporter, peers []int) *Node {
	return &Node{
		currentTerm: 0,

		leaderID: -1,
		votedFor: -1,

		store:     store,
		persister: persister,

		state: Follower,
		id:    id,

		transport: transport,

		electionTimeout:   time.Duration(150+rand.Intn(150)) * time.Millisecond,
		heartbeatInterval: time.Duration(50 * int(time.Millisecond)),

		peers:        peers,
		nextIndex:    make(map[int]uint64),
		syncResultCh: make(chan syncResult, len(peers)),
		commitIndex:  0,
		matchIndex:   make(map[int]uint64),
		lastApplied:  0,
		applyCh:      make(chan struct{}, 1),

		electionTimer:        &time.Timer{},
		heartbeatTimer:       &time.Timer{},
		resetElectionTimerCh: make(chan struct{}, 1),
	}
}

func (n *Node) Start() {
	state, err := n.persister.Load()
	if err != nil {
		panic(err)
	}

	n.mu.Lock()
	n.store.Restore(state.Blocks)
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor

	n.electionTimer = time.NewTimer(n.electionTimeout)
	n.heartbeatTimer = time.NewTimer(n.heartbeatInterval)
	n.heartbeatTimer.Stop()
	n.mu.Unlock()

	go n.applyLoop()
	go n.eventLoop()
}

func (n *Node) Propose(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		if n.leaderID != -1 {
			return fmt.Errorf("Not leader, leader is %d", n.leaderID)
		}
		return fmt.Errorf("Not leader, unknown leader.")
	}
	blk := storage.Block{
		Index:     n.store.LatestIndex() + 1,
		Term:      n.currentTerm,
		Timestamp: uint64(time.Now().UnixMilli()),
		Data:      storage.Transaction{Data: data},
	}
	n.store.Append(blk)
	return nil
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) rejectResp(term uint64) AppendEntriesResponse {
	return AppendEntriesResponse{Term: term, Success: false}
}

func (n *Node) successResp(term uint64) AppendEntriesResponse {
	return AppendEntriesResponse{Term: term, Success: true}
}

func (n *Node) rejectVoteResp(term uint64) RequestVoteResponse {
	return RequestVoteResponse{Term: term, VoteGranted: false}
}

func (n *Node) grantVoteResp(term uint64) RequestVoteResponse {
	return RequestVoteResponse{Term: term, VoteGranted: true}
}

func (n *Node) tryCommitByMajority() {
	matched := make([]uint64, 0, len(n.peers)+1)
	for _, m := range n.matchIndex {
		matched = append(matched, m)
	}
	latestIdx := n.store.LatestIndex()
	if latestIdx == 0 {
		return
	}
	matched = append(matched, latestIdx)
	slices.Sort(matched)
	majorityIdx := matched[len(matched)/2]
	if majorityIdx > n.commitIndex {
		if blk, ok := n.store.Get(majorityIdx); ok && blk.Term == n.currentTerm {
			n.setCommitIndex(majorityIdx)
		}
	}
}

func (n *Node) setCommitIndex(idx uint64) {
	if idx > n.commitIndex {
		n.commitIndex = idx
		trySend(n.applyCh, struct{}{})
	}
}

func (n *Node) applyLoop() {
	for range n.applyCh {
		n.mu.Lock()
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			blk, _ := n.store.Get(n.lastApplied)
			// stateMachine.Apply(blk.Data)
			_ = blk
		}
		n.mu.Unlock()
	}
}

func (n *Node) eventLoop() {
	for {
		select {
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				n.startElection()
			} else {
				n.mu.Unlock()
			}
		case <-n.heartbeatTimer.C:
			n.mu.Lock()
			if n.state == Leader {
				term := n.currentTerm
				leaderCommit := n.commitIndex
				n.mu.Unlock()
				n.sendAppendEntries(term, leaderCommit)
				n.heartbeatTimer.Reset(n.heartbeatInterval)
			} else {
				n.mu.Unlock()
			}
		case <-n.resetElectionTimerCh:
			n.mu.RLock()
			isFollower := n.state != Leader
			n.mu.RUnlock()
			if isFollower {
				n.electionTimer.Reset(n.electionTimeout)
			}
		case r := <-n.syncResultCh:
			n.mu.Lock()
			if r.count > 0 && r.success {
				n.nextIndex[r.id] += r.count
				n.matchIndex[r.id] = n.nextIndex[r.id] - 1
				n.tryCommitByMajority()
			} else if !r.success {
				if n.nextIndex[r.id] > 0 {
					n.nextIndex[r.id]--
				}
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) persisNow(term uint64, vote int) {
	blocks := n.store.SnapshotBlocks()
	if err := n.persister.Save(storage.PersistedState{
		CurrentTerm: term,
		VotedFor:    vote,
		Blocks:      blocks,
	}); err != nil {
		panic(err)
	}
}

func trySend[T any](ch chan<- T, val T) {
	select {
	case ch <- val:
	default:
	}
}
