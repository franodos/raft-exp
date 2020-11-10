package main

import (
	"encoding/json"
	"github.com/hashicorp/raft"
)

type Entry struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

type LogEntries map[string]string

type MySnapshot struct {
	LogEntries LogEntries
}

func (m *MySnapshot)Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(m.LogEntries)
	if err != nil {
		return sink.Cancel()
	}
	// 快照持久化
	_, err = sink.Write(data)
	if err != nil {
		return sink.Cancel()
	}
	if err = sink.Close(); err != nil {
		return sink.Close()
	}
	return nil
}

func (m *MySnapshot)Release() {}

