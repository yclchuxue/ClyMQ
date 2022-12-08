package main

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/raft_operations"
	Server "ClyMQ/server"
	"context"
	"fmt"
	"time"

	"net"
	"testing"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

func TestRaftRPC(t *testing.T) {

	var opts_raf []server.Option
	var peers []*raft_operations.Client
	raft_server := Server.NewParts_Raft()
	addr_raf, _ := net.ResolveTCPAddr("tcp", ":7788")
	opts_raf = append(opts_raf,  server.WithServiceAddr(addr_raf))

	aply := Server.GetServerInfoAply()
	go raft_server.Make("Broker1", opts_raf, aply, 1)
	time.Sleep(time.Second * 3)

	cli, err := raft_operations.NewClient("Broker1", client.WithHostPorts(":7788"))
	if err != nil {
		t.Fatal("new client err is ", err.Error())
	}

	raft_cli := &cli
	peers = append(peers, raft_cli)

	fmt.Println("send rpc to 7788")
	resp, err := (*peers[0]).Pingpongtest(context.Background(), &api.PingPongArgs_{
		Ping: true,
	})
	if err != nil || !resp.Pong{
		t.Fatal("new client err is ", err.Error())
	}

	fmt.Println("  ... Passed")
}