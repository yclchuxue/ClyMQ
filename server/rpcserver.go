package server

import (
	"ClyMQ/kitex_gen/api"
	"encoding/json"

	"ClyMQ/kitex_gen/api/server_operations"
	"context"
	"fmt"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	// me int64
	srv server.Server

	server *Server
}

func NewRpcServer() RPCServer {
	LOGinit()
	return RPCServer{
		server: NewServer(),
	}
}

func (s *RPCServer) Start(opts []server.Option) error {
	svr := server_operations.NewServer(s, opts...)
	s.srv = svr
	s.server.make()

	DEBUG(dLog, "Broker start rpcserver\n")
	err := svr.Run()

	if err != nil {
		fmt.Println(err.Error())
	}

	return nil
}

func (s *RPCServer)ShutDown_server(){
	s.srv.Stop()
}

func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {
	
	err = s.server.PushHandle(push{
		producer: req.Producer,
		topic: req.Topic,
		key: req.Key,
		message: req.Message,
	})
	if err == nil {
		return &api.PushResponse{Ret: true,}, nil
	}
	
	return &api.PushResponse{Ret: false,}, err
}

func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (resp *api.PullResponse, err error) {
	
	ret, err := s.server.PullHandle(pull{
		consumer: req.Consumer,
		topic: req.Topic,
		key: req.Key,
	})

	if err == nil {
		return &api.PullResponse{Message: ret.message}, nil
	}

	return &api.PullResponse{Message: "111"}, nil
}

func (s *RPCServer) Info(ctx context.Context, req *api.InfoRequest) (resp *api.InfoResponse, err error) {
	//get client_server's ip and port

	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{Ret: true}, nil;
	}
	
	return &api.InfoResponse{Ret: false}, err;
}

//订阅
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (resp *api.SubResponse, err error) {

	ret, err := s.server.SubHandle(sub{
		consumer: req.Consumer,
		topic: req.Topic,
		key: req.Key,
		option: req.Option,
	})

	data_parts, _ := json.Marshal(ret.parts)

	if err == nil{
		return &api.SubResponse{
			Ret: true,
			Size: int64(ret.size),
			Parts: data_parts,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (resp *api.InfoGetResponse, err error){
	err = s.server.StartGet(startget{
		cli_name: req.CliName,
		topic_name: req.TopicName,
		part_name: req.PartName,
		index: req.Offset,
	})

	if err == nil{
		return &api.InfoGetResponse{Ret: true}, nil
	}

	return &api.InfoGetResponse{Ret: false}, err
}