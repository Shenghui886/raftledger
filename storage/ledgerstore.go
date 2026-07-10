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

func (ls *LedgerStore) Get(index uint64) (Block, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if index >= uint64(len(ls.blocks)) {
		return Block{}, false
	}
	return ls.blocks[index], true
}

func (ls *LedgerStore) Length() uint64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	return uint64(len(ls.blocks))
}

func (ls *LedgerStore) Latest() (Block, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if len(ls.blocks) == 0 {
		return Block{}, false
	}
	return ls.blocks[len(ls.blocks)-1], true
}
