package storage

import (
	"sync"
)

type LedgerStore struct {
	mu     sync.RWMutex
	blocks []Block
}

func NewLedgerStore() *LedgerStore {
	return &LedgerStore{
		blocks: make([]Block, 0),
	}
}

func (ls *LedgerStore) Append(block Block) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.blocks = append(ls.blocks, block)
	return nil
}

func (ls *LedgerStore) Truncate(from uint64) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if from > 0 && from-1 < uint64(len(ls.blocks)) {
		ls.blocks = ls.blocks[:from-1]
	}
	return nil
}

func (ls *LedgerStore) Get(index uint64) (Block, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if index == 0 || index > uint64(len(ls.blocks)) {
		return Block{}, false
	}
	return ls.blocks[index-1], true
}

func (ls *LedgerStore) LatestIndex() uint64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	if l := len(ls.blocks); l == 0 {
		return 0
	} else {
		return ls.blocks[l-1].Index
	}
}

func (ls *LedgerStore) SnapshotBlocks() []Block {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	blocks := make([]Block, len(ls.blocks))
	copy(blocks, ls.blocks)

	return blocks
}

func (ls *LedgerStore) Restore(blocks []Block) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.blocks = blocks
}
