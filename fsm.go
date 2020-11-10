package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type MyFMS struct {
	mutex sync.Mutex
	LogEntries LogEntries
}

func (m *MyFMS)Apply(log *raft.Log) interface{}{
	m.mutex.Lock()
	data := log.Data
	fmt.Println(string(data))
	var logEntry Entry
	if err := json.Unmarshal(data, &logEntry); err != nil {
		panic("fail unmarshal log data!")
	}
	m.LogEntries[logEntry.Key] = logEntry.Value
	m.mutex.Unlock()
	return nil
}

// Snapshot 用于生成快照
func (m *MyFMS)Snapshot() (raft.FSMSnapshot, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	snapshot := &MySnapshot{m.LogEntries}
	return snapshot, nil
}

// Restore 从快照中恢复数据
func (m *MyFMS)Restore(read io.ReadCloser) error {
	var newData map[string]string
	if err := json.NewDecoder(read).Decode(&newData); err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.LogEntries = newData
	return nil
}


