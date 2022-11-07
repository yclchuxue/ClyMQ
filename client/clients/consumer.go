package clients

import (
	"ClyMQ/kitex_gen/api"
	ser "ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/server_operations"
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/cloudwego/kitex/server"
)

type Consumer struct {
	Cli  server_operations.Client   //连接多个broker
	Name string

	Topic_Partions map[string]Info
}

type Info struct {
	offset int64
	topic  string
	part   string
	// Cli    server_operations.Client  
	buffer []string
}

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req.Meg)
	return &api.PubResponse{Ret: true}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{Pong: true}, nil
}

func (c *Consumer) Start_server(port string) {
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(new(Consumer), opts...)

	err := svr.Run()
	if err != nil {
		println(err.Error())
	}
}

func (c *Consumer) StartGet() (err error) {

	ret := ""
	for tp_name, info := range c.Topic_Partions {
		req := api.InfoGetRequest{
			CliName: c.Name,
			TopicName: info.topic,
			PartName: info.part,
			Offset: info.offset,
		}
		resp, err := c.Cli.StarttoGet(context.Background(), &req)
		if err != nil || !resp.Ret{
			ret += tp_name + ": err != nil or resp.Ret == false\n"
		}
	}

	if ret == ""{
		return nil
	}else{
		return errors.New(ret)
	}
}
