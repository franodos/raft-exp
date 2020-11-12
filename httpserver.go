package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	ENABLE_WRITE_TRUE = int32(1)
	ENAVLE_WRITE_FALSE = int32(0)
)

type HttpServer struct {
	RaftServer *RaftServer
	EnableWrite int32
}

func NewHttpServer(raftServer *RaftServer) *HttpServer {
	return &HttpServer{
		RaftServer: raftServer,
		EnableWrite: ENAVLE_WRITE_FALSE,
	}
}

func (h *HttpServer) CheckEnableWrite() bool {
	return atomic.LoadInt32(&h.EnableWrite) == ENABLE_WRITE_TRUE
}

func (h *HttpServer) SetWriteFlag(flag bool) {
	if flag == true {
		atomic.StoreInt32(&h.EnableWrite, ENABLE_WRITE_TRUE)
	} else {
		atomic.StoreInt32(&h.EnableWrite, ENAVLE_WRITE_FALSE)
	}
}

func (h *HttpServer) Set(w http.ResponseWriter, r *http.Request){
	if h.CheckEnableWrite() == false {
		fmt.Fprintln(w, "current node is not master, can't write!")
		return
	}
	vars := r.URL.Query()
	key := vars.Get("key")
	value := vars.Get("value")

	//logEntries := LogEntries{key:value}
	logEntry := Entry{
		Key: key,
		Value: value,
	}
	if key == "" || value == "" {
		fmt.Fprintln(w, "set error, key or value is empty")
		return
	}
	logByte, err := json.Marshal(logEntry)
	if err != nil {
		fmt.Fprintln(w, "data Marshal error: "+err.Error())
		return
	}
	fmt.Printf("-----------开始写入: key = %s, value=%s\n", key, value)
	future := h.RaftServer.Raft.Apply(logByte, 5*time.Second)
	if err := future.Error(); err != nil {
		fmt.Fprintln(w, "data apply error: " + err.Error())
		return
	}
	fmt.Fprint(w, "OK")
}


func (h *HttpServer) Get(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	key := vars.Get("key")
	if key == "" {
		fmt.Fprintln(w, "get error, key is empty")
		return
	}
	value := h.RaftServer.FMS.LogEntries[key]
	fmt.Fprintln(w, value)
}
