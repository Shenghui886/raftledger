# raftledger

A distributed, append-only ledger built on a self-implemented Raft consensus protocol.

## Features

- **Raft consensus** — leader election, log replication, heartbeat-based failure detection
- **Append-only ledger** — blocks with term, timestamp, and arbitrary binary transactions
- **Follower redirect** — writes to non-leaders return the current leader's ID
- **Memory-first storage** — in-memory ledger, ready for persistence layer
- **Deterministic testing** — in-process `MemoryTransport` for race-free integration tests

## Implementation Status

| Component | Status |
| --------- | ------ |
| Leader election | ✅ |
| Log replication | ✅ |
| Follower redirect | ✅ |
| `PrevLogTerm` check | ❌ |
| `LeaderCommit` / `commitIndex` | ❌ |
| Log conflict resolution | ❌ |
| Persistence | ❌ |
| Network transport | ❌ |

See [TODO.md](TODO.md) for the full roadmap.

## Project Structure

```bash
raftledger/
├── main.go              # 3-node cluster test harness
├── raft/
│   ├── node.go          # Node struct, Start loop, Propose
│   ├── election.go      # Leader election logic
│   ├── replication.go   # Heartbeat + log replication
│   ├── state.go         # RPC request/response types
│   ├── transport.go     # Transport interface
│   └── memtransport.go  # In-process memory transport
├── storage/
│   ├── ledgerstore.go   # Append-only block store
│   ├── block.go         # Block type
│   └── transaction.go   # Transaction type
└── TODO.md              # Next steps
```

## Quick Start

```bash
# Build
go build -o raftledger .

# Run the 3-node cluster test
go run main.go

# Verify no data races
go run -race main.go
```

Expected output — all nodes consistent after 3 writes:

```bash
1. Leader Election
   ✓ Leader elected: Node 0

2. Follower Redirect
   ✓ Node 1 → Not leader, leader is 0
   ✓ Node 2 → Not leader, leader is 0
   2/2 followers rejected correctly

3. Write via Leader
   ✓ Written: tx1
   ✓ Written: tx2
   ✓ Written: tx3

4. Cross-Node Consistency
   ✓ All nodes consistent

5. Summary
   Node 0: 4 blocks
     [0] Term=1  Data=ping
     [1] Term=1  Data=tx1
     [2] Term=1  Data=tx2
     [3] Term=1  Data=tx3
   Node 1: 4 blocks
     [0] Term=1  Data=ping
     [1] Term=1  Data=tx1
     [2] Term=1  Data=tx2
     [3] Term=1  Data=tx3
   Node 2: 4 blocks
     [0] Term=1  Data=ping
     [1] Term=1  Data=tx1
     [2] Term=1  Data=tx2
     [3] Term=1  Data=tx3

All tests PASS
```

## Architecture

```txt
Client ──Propose()──► Leader ──sendAppendEntries()──► Followers
                         │
                    store.Append(block)     store.Append(block)
```

- `Propose` appends a block to the leader's local store
- Periodic heartbeats (50ms) replicate new blocks to followers
- `syncResultCh` tracks per-peer replication progress via `nextIndex`
- Election timeouts (150–300ms randomized) trigger automatic leader election

## License

[MIT](LICENSE)
