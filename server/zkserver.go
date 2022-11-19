package server

import (
	"ClyMQ/client/clients"
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/zookeeper"
	"context"
	"encoding/json"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type ZkServer struct {
	mu              sync.RWMutex
	zk              zookeeper.ZK
	Name            string
	Info_Brokers    map[string]zookeeper.BlockNode
	Info_Topics     map[string]zookeeper.TopicNode
	Info_Partitions map[string]zookeeper.PartitionNode

	Brokers map[string]server_operations.Client //连接各个broker
}

type Info_in struct {
	cli_name   string
	topic_name string
	part_name  string
	index      int64
	option     int8
}

type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	part_name     string
	index         int64
}

func NewZKServer(zkinfo zookeeper.ZkInfo) *ZkServer {
	return &ZkServer{
		mu: sync.RWMutex{},
		zk: *zookeeper.NewZK(zkinfo),
	}
}

func (z *ZkServer) make(opt Options) {
	z.Name = opt.Name
	z.Info_Brokers = make(map[string]zookeeper.BlockNode)
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)
}

func (z *ZkServer) HandleBroInfo(bro_name, bro_H_P string) error {
	bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(bro_H_P))
	if err != nil {
		DEBUG(dError, err.Error())
		return err
	}
	z.mu.Lock()
	z.Brokers[bro_name] = bro_cli
	z.mu.Unlock()

	return nil
}

func (z *ZkServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper，获得broker的host_port和name，若未连接则建立连接
	broker, block := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	z.mu.RLock()
	bro_cli, ok := z.Brokers[block.BrokerName]
	z.mu.RUnlock()

	//未连接该broker
	if !ok {
		bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(broker.HostPort))
		if err != nil {
			DEBUG(dError, err.Error())
		}
		z.mu.Lock()
		z.Brokers[broker.Name] = bro_cli
		z.mu.Unlock()
	}
	//通知broker检查topic/partition，并创建队列准备接收信息
	resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
		TopicName: block.TopicName,
		PartName:  block.PartitionName,
		FileName:  block.FileName,
	})
	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error()+resp.Err)
	}
	//返回producer broker的host_port
	return Info_out{
		Err:           err,
		broker_name:   broker.Name,
		bro_host_port: broker.HostPort,
	}
}

func (z *ZkServer) CreateTopic(info Info_in) Info_out {

	//可添加限制数量等操作
	tnode := zookeeper.TopicNode{
		Name: info.topic_name,
	}
	err := z.zk.RegisterNode(tnode)
	return Info_out{
		Err: err,
	}
}

func (z *ZkServer) CreatePart(info Info_in) Info_out {

	//可添加限制数量等操作
	pnode := zookeeper.PartitionNode{
		Name: info.part_name,
		TopicName: info.topic_name,
		PTPoffset: int64(0),
	}

	err := z.zk.RegisterNode(pnode)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
	//创建NowBlock节点，接收信息
	err = z.CreateNowBlock(info)
	return Info_out{
		Err: err,
	}
}

func (z *ZkServer)CreateNowBlock(info Info_in) error {
	brock_node := zookeeper.BlockNode{
		Name: "NowBlock",
		FileName: info.topic_name+info.part_name+"now.txt",
		TopicName: info.topic_name,
		PartitionName: info.part_name,
		StartOffset: int64(0),
	}
	return z.zk.RegisterNode(brock_node)
}

func (z *ZkServer)SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中

	return nil
}

//consumer查询该向那些broker发送请求
//zkserver让broker准备好topic/sub和config
func (z *ZkServer)HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var partkeys []clients.PartKey
	var Parts 	 []zookeeper.Part
	/* 
	检查该应户是否订阅了该topic/partition
	......
	*/
	z.zk.CheckSub(zookeeper.StartGetInfo{
		Cli_name: info.cli_name,
		Topic_name: info.topic_name,
		PartitionName: info.part_name,
		Option:	info.option,
	})
	
	if info.option == 1 { //ptp
		Parts, err = z.zk.GetBrokers(info.topic_name)
	}else if info.option == 3 { //psb
		Parts, err = z.zk.GetBroker(info.topic_name, info.part_name, info.index)
	}
	if err != nil {
		return nil, 0, err
	}

	//获取到信息后将通知brokers，让他们检查是否有该Topic/Partition/Subscription/config等
	//并开启Part发送协程，若协程在超时时间到后未收到管道的信息，则关闭该协程
	for _, part := range Parts {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err = server_operations.NewClient(z.Name, client.WithHostPorts(part.Host_Port))
			if err != nil {
				DEBUG(dError, "broker(%v) host_port(%v) con't connect %v",part.BrokerName, part.Host_Port, err.Error())
			}
			z.mu.Lock()
			z.Brokers[part.BrokerName] = bro_cli
			z.mu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			TopicName: info.topic_name,
			PartName: part.Part_name,
			FileName: part.File_name,
			Option: info.option,
		}
		if rep.Option == 1 { //ptp
			rep.Offset = part.PTP_index
		}else if rep.Option == 3 { //psb
			rep.Offset = info.index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			DEBUG(dError, "PrepareSend err(%v) error %v", resp.Err, err.Error())
		}

		partkeys = append(partkeys, clients.PartKey{
			Name: part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P: part.Host_Port,
		})
	}

	data, err := json.Marshal(partkeys)
	if err != nil {
		DEBUG(dError, "turn partkeys to json faile %v", err.Error())
	}

	return data, len(partkeys), nil
}

//发送请求到broker，关闭该broker上的partition的接收程序
//并修改NowBlock的文件名，并修改zookeeper上的block信息
func (z *ZkServer)CloseAcceptPartition() {

}