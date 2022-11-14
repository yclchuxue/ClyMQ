package main

import (
	"ClyMQ/kitex_gen/api"
	"context"
	"fmt"
	"time"

	"testing"
)

func TestConsumerNet(t *testing.T) {

	fmt.Println("Test: consumer net")

	server_port := ":7778"
	consumer_port := ":8000"
	rpcserver := NewBrokerAndStart(t, server_port)

	consumer := NewConsumerAndStart(t, server_port, consumer_port)

	info := &api.InfoRequest{
		IpPort: "0.0.0.0" + consumer_port,
	}

	time.Sleep(3*time.Second)

	resp, err := consumer.Cli.Info(context.Background(), info)
	if err != nil {
		fmt.Println(resp)
	}

	consumer.ShutDown_server()
	rpcserver.ShutDown_server()

	fmt.Println("  ... Passed")
}
