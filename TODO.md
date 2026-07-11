# TODO

## Raft 协议规范

- [x] AppendEntries 增加 `PrevLogTerm` 字段，follower 检测 term 冲突
- [x] `LeaderCommit` + `commitIndex`，follower 按需应用已提交日志
- [x] `HandleRequestVote` 拒绝时也重置选举超时
- [x] 日志冲突时 follower 删除冲突 entry 及其后续（当前只能 append）
- [x] 将 store 的索引从 0-indexed 改为 1-indexed，消除 `PrevLogIndex=0` 的哨兵歧义
- [x] 选举时 leader 发空 `AppendEntries` 建立权威（当前在 `startElection` 结尾已有）

## 持久化

- [ ] `storage` 层支持从磁盘读写（当前纯内存）
- [ ] 节点重启后恢复 `currentTerm`、`votedFor`、日志

## 网络传输

- [ ] 实现 `Transport` 接口的 gRPC/TCP 版本（当前仅 `MemoryTransport`）

## 快照

- [ ] 日志压缩与快照安装
