package main

import (
	"ClyMQ/kitex_gen/api/server_operations"
	"fmt"

	"testing"
	"time"

	client3 "ClyMQ/client/clients"

	client2 "github.com/cloudwego/kitex/client"
)

//测试该测试点需要将MQServer启动
func TestProducerNet(t *testing.T) {

	fmt.Println("Test: Producer net")

	port := ":7778"
	rpcserver := NewBrokerAndStart(t, port)

	time.Sleep(2*time.Second)

	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0"+port))

	if err != nil {
		t.Fatal(err)
	}

	producer := client3.Producer{}
	producer.Name = client3.GetIpport() + "producter_test1"
	producer.Cli = client

	err = producer.Push(client3.Message{
		Topic_name: "phone_number",
		Part_name:  "ycl",
		Msg:        "18788888888",
	})

	if err != nil {
		t.Fatal(err)
	}

	rpcserver.ShutDown_server()
	
	fmt.Println("  ... Passed")
}
