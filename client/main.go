package main

import (
	"ClyMQ/kitex_gen/api"
	ser "ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/server_operations"
	"context"
	"fmt"
	"net"
	"time"

	client2 "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

type Server struct{

}

func (s *Server)Pub(ctx context.Context, req *api.PubRequest)(resp *api.PubResponse, err error){
	fmt.Println(req.Meg)
	return &api.PubResponse{Ret: true}, nil
}

func (s *Server)Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{Pong: true}, nil
}

func start_server(port string){
	addr,_ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(new(Server), opts...)

	err := svr.Run()
	if err != nil {
		println(err.Error())
	}
}

func main() {
	
	//start a server for pub and pinpong
	go start_server(":8889")

	//connection the broker server for push/pull/info
	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0:8888"))
	if err != nil {
		fmt.Println(err)
	}

	//send ip and port for brokerserver can pub this client
	info := &api.InfoRequest{
		IpPort: "0.0.0.0:8889",
	}
	resp, err := client.Info(context.Background(), info)
	if(err != nil){
		fmt.Println(resp)
	}

	//test
	for {
		req := &api.PushRequest{
			Producer: int64(1),
			Topic:    "phone number",
			Key:      "yclchuxue",
			Message:  "18788888888",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(5*time.Second)
	}
}
