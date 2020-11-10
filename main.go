package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
)

var (
	curRaftConfig RaftConfig
	curMyConfig MyConfig
)

/*
.\myraft.exe --http-addr='127.0.0.1:7000' --raft-addr='127.0.0.1:7001' --raft-id=1 --cluster-addr='1/127.0.0.1:7001,2/127.0.0.1:8001,3/127.0.0.1:9001'
.\myraft.exe --http-addr='127.0.0.1:8000' --raft-addr='127.0.0.1:8001' --raft-id=2 --cluster-addr='1/127.0.0.1:7001,2/127.0.0.1:8001,3/127.0.0.1:9001'
.\myraft.exe --http-addr='127.0.0.1:9000' --raft-addr='127.0.0.1:9001' --raft-id=3 --cluster-addr='1/127.0.0.1:7001,2/127.0.0.1:8001,3/127.0.0.1:9001'
*/

func init()  {
	var httpAddr string
	var raftAddr string
	var raftID string
	var clusterAddr string
	flag.StringVar(&httpAddr, "http-addr", "127.0.0.1:7000", "http listen address")
	flag.StringVar(&raftAddr, "raft-addr", "127.0.0.1:7001", "raft tcp address")
	flag.StringVar(&raftID, "raft-id", "1", "raft server id")
	flag.StringVar(&clusterAddr, "cluster-addr", "1/127.0.0.1:70001,2/127.0.0.1:8001,3/127.0.0.1:9001", "raft cluster address")
	flag.Parse()
	fmt.Printf("参数：http-addr=%s raft-addr=%s raft-id=%s cluster-addr=%s", httpAddr, raftAddr, raftID, clusterAddr)
	curRaftConfig.HttpAddr = httpAddr
	curRaftConfig.RaftAddr = raftAddr
	curRaftConfig.RaftID = raftID
	//dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	//if err != nil {
	//	panic(dir)
	//}
	dir := "node/raft_" + raftID
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		panic(err)
	}
	curRaftConfig.DataDir = dir

	addrArray := strings.Split(clusterAddr, ",")
	if len(addrArray) == 0 {
		panic("don't set cluster address")
	}
	for _, addrInfo := range addrArray {
		nodeAddr := strings.Split(addrInfo, "/")
		raftConfig := RaftConfig{
			RaftID: nodeAddr[0],
			RaftAddr: nodeAddr[1],
		}
		curMyConfig.RaftConfig = append(curMyConfig.RaftConfig, raftConfig)
	}
}

func main()  {
	// 初始化raft
	fmt.Println("初始化raft", curRaftConfig.RaftAddr, curRaftConfig.RaftID)
	raftServer := NewRaftSever(&curRaftConfig)
	// 启动raft
	fmt.Println("启动raft")
	Bootstrap(raftServer, &curMyConfig)

	// 启动http
	fmt.Println("启动http服务")
	httpServer := NewHttpServer(raftServer)

	go func() {
		for leader := range raftServer.Raft.LeaderCh() {
			httpServer.SetWriteFlag(leader)
		}
	}()

	http.HandleFunc("/set", httpServer.Set)
	http.HandleFunc("/get", httpServer.Get)
	if err := http.ListenAndServe(curRaftConfig.HttpAddr, nil); err != nil {
		panic(err)
	}

	future := raftServer.Raft.Shutdown()
	if err := future.Error(); err != nil {
		panic("shutdown raft error: " + err.Error())
	}
	fmt.Println("shutdown raft server!")
}