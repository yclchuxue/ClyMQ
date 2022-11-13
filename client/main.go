package main

import (
	client3 "ClyMQ/client/clients"
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"context"
	"fmt"
	"os"

	// "net"
	"time"

	client2 "github.com/cloudwego/kitex/client"
)

func main() {
	
	//connection the broker server for push/pull/info
	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0:8888"))
	if err != nil {
		fmt.Println(err)
	}

	option := os.Args[1]
	port := ""
	if len(os.Args) == 3{
		port = os.Args[2]
	}else{
		port = "null"
	}

	ipport := ""

	switch option{
	case "p":
		producer := client3.Producer{}
		producer.Name = client3.GetIpport() + port
		producer.Cli = client
		ipport = producer.Name
	case "c":	
		consumer := client3.NewConsumer()
		//start a server for pub and pinpong
		go consumer.Start_server(":"+port)
		consumer.Name = client3.GetIpport() + port
		consumer.Cli = client
		ipport = consumer.Name
	}

	//send ip and port for brokerserver can pub this client
	info := &api.InfoRequest{
		IpPort: port,
	}
	resp, err := client.Info(context.Background(), info)
	if err != nil {
		fmt.Println(resp)
	}

	//test
	for {
		req := &api.PushRequest{
			Producer: ipport,
			Topic:    "phone number",
			Key:      "yclchuxue",
			Message:  "18788888888",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(5 * time.Second)
	}
}
