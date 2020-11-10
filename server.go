package main

import (
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

type RaftConfig struct {
	HttpAddr string
	RaftAddr string
	RaftID   string
	DataDir  string
}

type MyConfig struct {
	RaftConfig []RaftConfig
}

type RaftServer struct {
	Raft *raft.Raft
	FMS *MyFMS
}

func NewRaftSever(raftConfig *RaftConfig) *RaftServer {
	config := raft.DefaultConfig()
	// 提交5个日志之后就生成快照
	config.SnapshotThreshold = 5
	config.SnapshotInterval = 60 * time.Second
	config.LocalID = raft.ServerID(raftConfig.RaftID)
	fsm := new(MyFMS)
	fsm.LogEntries = make(map[string]string)
	logStore, err := boltdb.NewBoltStore(filepath.Join(raftConfig.DataDir, "ratf-log.bolt"))
	if err != nil {
		panic(err)
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(raftConfig.DataDir, "ratf-stable.bolt"))
	if err != nil {
		panic(err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(raftConfig.DataDir, 1, os.Stderr)
	if err != nil {
		panic(err)
	}
	advertise, err := net.ResolveTCPAddr("tcp", raftConfig.RaftAddr)
	trans, err := raft.NewTCPTransport(raftConfig.RaftAddr, advertise,3, 10*time.Second, os.Stderr)
	if err != nil {
		panic(err)
	}

	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, trans)
	if err != nil {
		panic(err)
	}

	return &RaftServer{
		Raft: raftNode,
		FMS: fsm,
	}
}


func Bootstrap(raftNode *RaftServer, myConfig *MyConfig) {
	var configuration raft.Configuration
	for _, raftConfig := range myConfig.RaftConfig {
		server := raft.Server{
			ID: raft.ServerID(raftConfig.RaftID),
			Address: raft.ServerAddress(raftConfig.RaftAddr),
		}
		configuration.Servers = append(configuration.Servers, server)
	}
	raftNode.Raft.BootstrapCluster(configuration)
}

