package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"ClyMQ/zookeeper"
	"context"
	"fmt"

	"github.com/cloudwego/kitex/server"
)

//RPCServer可以注册两种server，
//一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
//另一个是Broker获取Zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	// me int64
	name 			string
	srv_cli      	server.Server
	srv_bro 	 	server.Server
	zkinfo			zookeeper.ZkInfo
	server   		*Server
	zkserver 		*ZkServer
}

func NewRpcServer(zkinfo zookeeper.ZkInfo) RPCServer {
	LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

func (s *RPCServer) Start(opts_cli, opts_bro []server.Option, opt Options) error {

	switch opt.Tag {
	case BROKER :
		s.server = NewServer(s.zkinfo)
		s.server.make(opt)
	case ZKBROKER:
		s.zkserver = NewZKServer(s.zkinfo)
		s.zkserver.make(opt)

		srv_bro := zkserver_operations.NewServer(s, opts_bro...)
		s.srv_bro = srv_bro
		DEBUG(dLog, "Broker start rpcserver for brokers\n")
		go func() {
			err := srv_bro.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}

	srv_cli := server_operations.NewServer(s, opts_cli...)
	s.srv_cli = srv_cli
	DEBUG(dLog, "Broker start rpcserver for clients\n")
	go func() {
		err := srv_cli.Run()

		if err != nil {
			fmt.Println(err.Error())
		}
	}()

	return nil
}

func (s *RPCServer) ShutDown_server() {
	s.srv_cli.Stop()
	s.srv_bro.Stop()
}

func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {

	err = s.server.PushHandle(push{
		producer: req.Producer,
		topic:    req.Topic,
		key:      req.Key,
		message:  req.Message,
	})
	if err == nil {
		return &api.PushResponse{Ret: true}, nil
	}

	return &api.PushResponse{Ret: false}, err
}

func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (resp *api.PullResponse, err error) {

	ret, err := s.server.PullHandle(pull{
		consumer: req.Consumer,
		topic:    req.Topic,
		key:      req.Key,
	})

	if err == nil {
		return &api.PullResponse{Message: ret.message}, nil
	}

	return &api.PullResponse{Message: "111"}, nil
}

func (s *RPCServer) ConInfo(ctx context.Context, req *api.InfoRequest) (resp *api.InfoResponse, err error) {
	//get client_server's ip and port

	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{Ret: true}, nil
	}

	return &api.InfoResponse{Ret: false}, err
}

//订阅
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (resp *api.SubResponse, err error) {

	err = s.server.SubHandle(sub{
		consumer: req.Consumer,
		topic:    req.Topic,
		key:      req.Key,
		option:   req.Option,
	})

	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (resp *api.InfoGetResponse, err error) {
	err = s.server.StartGet(startget{
		cli_name:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		index:      req.Offset,
	})

	if err == nil {
		return &api.InfoGetResponse{Ret: true}, nil
	}

	return &api.InfoGetResponse{Ret: false}, err
}

func (s *RPCServer) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (r *api.ProGetBrokResponse, err error) {

}

func (s *RPCServer) ConGetBroker(ctx context.Context, req *api.ConGetBrokRequest) (r *api.ConGetBrokResponse, err error) {

}

func (s *RPCServer)	BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	
}

func (s *RPCServer) BroGetConfig(ctx context.Context, req *api.BroGetConfigRequest) (r *api.BroGetConfigResponse, err error) {
	
}