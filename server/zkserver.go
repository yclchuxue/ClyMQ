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
	dupnum 	   int8
}

type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret 		  string
	// part_name     string
	// index         int64
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

//broker连接到zkserver，zkserver将在zookeeper上监听broker的状态
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

//producer Get Broker
func (z *ZkServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper，获得broker的host_port和name，若未连接则建立连接
	broker, block := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)

	z.mu.RLock()
	bro_cli, ok := z.Brokers[block.LeaderBroker]
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

//创建Partition
func (z *ZkServer) CreatePart(info Info_in) Info_out {

	//可添加限制数量等操作
	pnode := zookeeper.PartitionNode{
		Name: info.part_name,
		TopicName: info.topic_name,
		Option: -2,
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

//设置Partition的接收信息方式
//若ack = -1,则为raft同步信息
//若ack = 1, 则leader写入,	fetch获取信息
//若ack = 0, 则立即返回,   	fetch获取信息
func (z *ZkServer)SetPartitionState(info Info_in) Info_out {
	var ret string
	node := z.zk.GetPartState(info.topic_name, info.part_name)

	if info.option != node.Option {
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topic_name,
			Name: info.part_name,
			Option: info.option,
			PTPoffset: node.PTPoffset,
		})
	}

	switch info.option {
	case -1:
		if node.Option == -1 {  //与原来的状态相同
			ret = "HadRaft" 
		}
		if node.Option == 1 || node.Option == 0 {	//原状态为fetch, 关闭原来的写状态, 创建新raft集群
			//查询raft集群的broker, 发送信息
			// 关闭raft集群, 开启fetch操作,不需要更换文件

		}
		if node.Option == -2 {  //未创建任何状态, 即该partition未接收过任何信息
			//负载均衡获得一定数量broker节点,并在这些broker上部署raft集群 
			var brokers []*server_operations.Client
			for _, broker := range brokers {
				broker.
			}
		}

	default:
		if node.Option != -1 {	//与原状态相同
			ret = "HadFetch"
		}else{          //由raft改为fetch
			//查询fetch的Broker, 发送信息
			//关闭fetch操作, 创建raft集群,需要更换文件

		}	

		if node.Option == -2 {  //未创建任何状态, 即该partition未接收过任何信息
			//负载均衡获得一定数量broker节点,选择一个leader, 并让其他节点fetch leader信息
		}

	}
	return Info_out{
		Ret: ret,
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
//zkserver让broker准备好topic/sub和config    //Pub版
func (z *ZkServer)HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
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
	
	//获取该topic或partition的broker,并保证在线,若全部离线则Err
	if info.option == 1 { //ptp_push
		Parts, err = z.zk.GetBrokers(info.topic_name)
	}else if info.option == 3 { //psb_push
		Parts, err = z.zk.GetBroker(info.topic_name, info.part_name, info.index)
	}
	if err != nil {
		return nil, 0, err
	}

	//获取到信息后将通知brokers，让他们检查是否有该Topic/Partition/Subscription/config等
	//并开启Part发送协程，若协程在超时时间到后未收到管道的信息，则关闭该协程
	// var partkeys []clients.PartKey
	// if info.option == 1 || info.option == 3 {
	partkeys := z.SendPreoare(Parts, info)
	// }else{
		// partkeys = GetPartKeys(Parts)
	// }

	data, err := json.Marshal(partkeys)
	if err != nil {
		DEBUG(dError, "turn partkeys to json faile %v", err.Error())
	}

	return data, len(partkeys), nil
}

//push
func (z *ZkServer)SendPreoare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey){

	for _, part := range Parts {
		if part.Err != OK {
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(part.Host_Port))
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
			Err: OK,
		})
	}

	return partkeys
}

func (z *ZkServer)UpdateOffset(info Info_in) error {
	err := z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name: info.part_name,
		TopicName: info.topic_name,
		PTPoffset: info.index,
	})
	return err
}

func GetPartKeys(Parts []zookeeper.Part) (partkeys []clients.PartKey){
	for _, part := range Parts  {
		partkeys = append(partkeys, clients.PartKey{
			Name: part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P: part.Host_Port,
		})
	}
	return partkeys
}

//发送请求到broker，关闭该broker上的partition的接收程序
//并修改NowBlock的文件名，并修改zookeeper上的block信息
func (z *ZkServer)CloseAcceptPartition() {

}