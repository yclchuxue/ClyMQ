package main

import (
	Server "ClyMQ/server"
	"net"
	"testing"

	"github.com/cloudwego/kitex/server"
	"ClyMQ/kitex_gen/api/server_operations"
	client2 "github.com/cloudwego/kitex/client"
	client3 "ClyMQ/client/clients"
)

func NewBrokerAndStart(t *testing.T, port string) *Server.RPCServer {
	//start the broker server
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := Server.NewRpcServer()

	go func() {
		err := rpcServer.Start(opts)
		if err != nil {
			t.Log(err)
		}
	}()

	return &rpcServer
}

func NewConsumerAndStart(t *testing.T, server_port, consumer_port string) *client3.Consumer {
	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0"+server_port))

	if err != nil {
		t.Fatal(err)
	}

	consumer := client3.NewConsumer()
	consumer.Name = client3.GetIpport() + consumer_port
	consumer.Cli = client

	go consumer.Start_server(consumer_port)

	return &consumer
}