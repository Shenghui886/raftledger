package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Shenghui886/raftledger/storage"
)

type Node struct {
	mu sync.RWMutex

	currentTerm uint64

	votedFor int
	leaderID int

	store *storage.LedgerStore
	state NodeState
	id    int

	transport Transport

	electionTimeout   time.Duration
	heartbeatInterval time.Duration

	peers        []int
	nextIndex    map[int]uint64
	syncResultCh chan syncResult

	electionTimer        *time.Timer
	heartbeatTimer       *time.Timer
	resetElectionTimerCh chan struct{}
}

type syncResult struct {
	id    int
	count uint64
}

func NewNode(id int, store *storage.LedgerStore, transport Transport, peers []int) *Node {
	return &Node{
		currentTerm: 0,

		leaderID: -1,
		votedFor: -1,

		store: store,
		state: Follower,
		id:    id,

		transport: transport,

		electionTimeout:   time.Duration(150+rand.Intn(150)) * time.Millisecond,
		heartbeatInterval: time.Duration(50 * int(time.Millisecond)),

		peers:        peers,
		nextIndex:    make(map[int]uint64),
		syncResultCh: make(chan syncResult, len(peers)),

		electionTimer:        &time.Timer{},
		heartbeatTimer:       &time.Timer{},
		resetElectionTimerCh: make(chan struct{}, 1),
	}
}

func (n *Node) Start() {
	n.mu.Lock()
	n.electionTimer = time.NewTimer(n.electionTimeout)
	n.heartbeatTimer = time.NewTimer(n.heartbeatInterval)
	n.heartbeatTimer.Stop()
	n.mu.Unlock()

	go func() {
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
					n.mu.Unlock()
					n.sendHeartbeat(term)
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
				if r.count > 0 {
					n.nextIndex[r.id] += r.count
				} else {
					if n.nextIndex[r.id] > 0 {
						n.nextIndex[r.id]--
					}
				}
			}
		}
	}()
}

func (n *Node) Propose(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	blk := storage.Block{
		Index:     n.store.Length(),
		Term:      n.currentTerm,
		Timestamp: uint64(time.Now().UnixMilli()),
		Data:      storage.Transaction{Data: data},
	}

	if n.state == Leader {
		return n.store.Append(blk)
	}
	if n.leaderID != -1 {
		return fmt.Errorf("Not leader, leader is %d", n.leaderID)
	}

	return fmt.Errorf("Not leader, unknown leader.")
}

func (n *Node) ID() int {
	return n.id
}
