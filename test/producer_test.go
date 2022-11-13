package main

import (
	"ClyMQ/kitex_gen/api/server_operations"
	Server "ClyMQ/server"
	"net"
	"testing"

	client3 "ClyMQ/client/clients"

	client2 "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

func NewBrokerAndStart(t *testing.T, port string) {
	//start the broker server
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := Server.NewRpcServer()

	err := rpcServer.Start(opts)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNet(t *testing.T) {
	port := ":7778"
	NewBrokerAndStart(t, port)

	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0"+port))

	if err != nil {
		t.Fatal(err)
	}

	producer := client3.Producer{}
	producer.Name = client3.GetIpport() + "producter_test1"
	producer.Cli = client

	err = producer.Push(client3.Message{
		Topic_name: "phone_number",
		Part_name:  "yclchuxue",
		Msg:        "18788888888",
	})

	if err != nil {
		t.Fatal(err)
	}
}
