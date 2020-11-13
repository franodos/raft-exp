package raft

import (
	"sort"
	"sync"
	"fmt"
)

// Commitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with Match(), and
// this notifies on commitCh when the commit index has advanced.
type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	// 保存了各个节点，已经追加成功的日志的最新索引
	// matchIndexs 应该是保存着foller节点与leader完全匹配的日志的最大的索引
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	// leader 任期内的第一个索引，在进行任何提交之前这个值需要发送给集群的大多数成员
	startIndex uint64
}

// newCommitment returns an commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
// match 一旦log entires被写入磁盘match就会被调用，不管leader是不已写入新的entry，或有follower 应答了一个
// ApenndEntires RPC，
// 参数matchIndex表示该索引的日志已经被写入磁盘，
// 将该参数更新到c.machIndexes中，然后检查是否大多数节点都更新了该索引日志
// 如果是，就通知提交
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	fmt.Printf(">>>>server=%+v, mathchIndex=%+v, matchIndexes=%+v\n", server, matchIndex, c.matchIndexes)
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// Internal helper to calculate new commitIndex from matchIndexes.
// Must be called with lock held.
func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]

	fmt.Println(">>>>quorumMatchIndex = ", quorumMatchIndex)
	fmt.Println(">>>>c.commitIndex =", c.commitIndex)
	fmt.Println(">>>>c.startIndex = ", c.startIndex)

	// 有超过一半的节点appendEntries成功，那么就提交日志
	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotifyCh(c.commitCh)
	}
}
