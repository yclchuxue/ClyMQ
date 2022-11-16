package server

import (
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/zookeeper"
	"sync"
)


type ZkServer struct {
	mu 		sync.RWMutex
	zk 		zookeeper.ZK

	Info_Brokers 		map[string]zookeeper.BlockNode
	Info_Topics 		map[string]zookeeper.TopicNode	
	Info_Partitions 	map[string]zookeeper.PartitionNode

	Brokers 			map[string]server_operations.Client  //连接各个broker
}

func NewZKServer(zkinfo zookeeper.ZkInfo) *ZkServer {
	return &ZkServer{
		mu: 	sync.RWMutex{},
		zk:		*zookeeper.NewZK(zkinfo),
	}
}

func (z *ZkServer)make(opt Options) {
	z.Info_Brokers = make(map[string]zookeeper.BlockNode)
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)

	
}