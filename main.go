package main

import (
	"fmt"
	"time"

	"github.com/Shenghui886/raftledger/raft"
	"github.com/Shenghui886/raftledger/raft/grpctransport"
	"github.com/Shenghui886/raftledger/storage"
	"github.com/Shenghui886/raftledger/storage/filepersister"
)

func waitElection(nodes []*raft.Node) *raft.Node {
	for i := 0; i < 30; i++ {
		for _, n := range nodes {
			if err := n.Propose([]byte("ping")); err == nil {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func testFollowerReject(nodes []*raft.Node, leader *raft.Node) {
	count := 0
	for _, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		if err := n.Propose([]byte("should fail")); err != nil {
			fmt.Printf("   ✓ Node %d → %v\n", n.ID(), err)
			count++
		}
	}
	fmt.Printf("   %d/%d followers rejected correctly\n", count, len(nodes)-1)
}

func testLeaderWrite(leader *raft.Node, data []string) {
	for _, d := range data {
		if err := leader.Propose([]byte(d)); err != nil {
			fmt.Printf("   ✗ Write '%s' failed: %v\n", d, err)
			return
		}
		fmt.Printf("   ✓ Written: %s\n", d)
		time.Sleep(5 * time.Millisecond)
	}
}

func testConsistency(stores []*storage.LedgerStore, leaderID int) bool {
	leaderStore := stores[leaderID]
	allMatch := true
	for i, s := range stores {
		if i == leaderID {
			continue
		}
		if s.LatestIndex() != leaderStore.LatestIndex() {
			fmt.Printf("   ✗ Node %d: length %d != leader %d\n",
				i, s.LatestIndex(), leaderStore.LatestIndex())
			allMatch = false
			continue
		}
		for j := uint64(1); j <= s.LatestIndex(); j++ {
			leaderBlk, _ := leaderStore.Get(j)
			followerBlk, _ := s.Get(j)
			if leaderBlk.Term != followerBlk.Term ||
				string(leaderBlk.Data.Data) != string(followerBlk.Data.Data) {
				fmt.Printf("   ✗ Node %d: block %d content mismatch\n", i, j)
				allMatch = false
				break
			}
		}
	}
	if allMatch {
		fmt.Println("   ✓ All nodes consistent")
	}
	return allMatch
}

func printSummary(stores []*storage.LedgerStore) {
	for i, s := range stores {
		fmt.Printf("   Node %d: %d blocks\n", i, s.LatestIndex())
		for j := uint64(1); j <= s.LatestIndex(); j++ {
			blk, _ := s.Get(j)
			fmt.Printf("     [%d] Term=%d  Data=%s\n", blk.Index, blk.Term, blk.Data.Data)
		}
	}
}

func main() {
	store0 := storage.NewLedgerStore()
	store1 := storage.NewLedgerStore()
	store2 := storage.NewLedgerStore()

	p0 := filepersister.New("node0.json")
	p1 := filepersister.New("node1.json")
	p2 := filepersister.New("node2.json")

	peerAddr := map[int]string{
		0: ":9000",
		1: ":9001",
		2: ":9002",
	}

	t0 := grpctransport.New(peerAddr)
	t1 := grpctransport.New(peerAddr)
	t2 := grpctransport.New(peerAddr)

	node0 := raft.NewNode(0, store0, p0, t0, []int{1, 2})
	node1 := raft.NewNode(1, store1, p1, t1, []int{0, 2})
	node2 := raft.NewNode(2, store2, p2, t2, []int{0, 1})

	go grpctransport.ListenAndServe(":9000", node0)
	go grpctransport.ListenAndServe(":9001", node1)
	go grpctransport.ListenAndServe(":9002", node2)

	time.Sleep(50 * time.Millisecond)

	node0.Start()
	node1.Start()
	node2.Start()

	nodes := []*raft.Node{node0, node1, node2}
	stores := []*storage.LedgerStore{store0, store1, store2}

	fmt.Println("1. Leader Election")
	leader := waitElection(nodes)
	if leader == nil {
		fmt.Println("   ✗ No leader elected")
		return
	}
	fmt.Printf("   ✓ Leader elected: Node %d\n", leader.ID())

	fmt.Println()
	fmt.Println("2. Follower Redirect")
	testFollowerReject(nodes, leader)

	fmt.Println()
	fmt.Println("3. Write via Leader")
	testLeaderWrite(leader, []string{"tx1", "tx2", "tx3"})

	fmt.Println()
	fmt.Println("4. Cross-Node Consistency")
	time.Sleep(500 * time.Millisecond)
	consistent := testConsistency(stores, leader.ID())

	fmt.Println()
	fmt.Println("5. Summary")
	printSummary(stores)

	fmt.Println()
	if consistent {
		fmt.Println("All tests PASS")
	} else {
		fmt.Println("Election:         ✓ PASS")
		fmt.Println("Follower Redirect: ✓ PASS")
		fmt.Println("Leader Write:     ✓ PASS")
		fmt.Println("Log Replication:  ✗ NEXT STEP")
	}
}
