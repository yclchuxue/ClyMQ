package server

import (
	"ClyMQ/client/clients"
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/logger"
	"ClyMQ/zookeeper"
	"context"
	"encoding/json"
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
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

	//每个partition所在的集群   topic+partition to brokers
	PartToBro map[string][]string
	// PartStatus  map[string]bool
	consistent *ConsistentBro
}

type Info_in struct {
	cli_name   string
	topic_name string
	part_name  string
	blockname  string
	index      int64
	option     int8
	dupnum     int8
	// leader     bool
}

type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret           string
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
	z.PartToBro = make(map[string][]string)

	z.consistent = NewConsistentBro()
}

//broker连接到zkserver，zkserver将在zookeeper上监听broker的状态
func (z *ZkServer) HandleBroInfo(bro_name, bro_H_P string) error {
	bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(bro_H_P))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	z.mu.Lock()
	z.Brokers[bro_name] = bro_cli
	z.mu.Unlock()

	//加入consistent中进行负载均衡
	z.consistent.Add(bro_name, 1)

	//增加动态扩容机制，若有新broker加入应动态进行扩容
	//既对一些服务进行动态转移----producer服务
	//对于raft集群将集群停止并开启负载均衡完后将重新启动这个集群
	//对于fetch机制同上，暂时停止服务后迁移副本broker，负载均衡

	//对consumer服务保持正常状态，继续提供服务

	return nil
}

func (z *ZkServer) RebalancePtoB() {

}

func (z *ZkServer) Update() {

}

//producer Get Broker
func (z *ZkServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper，获得broker的host_port和name，若未连接则建立连接
	broker, block, _, err := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	PartitionNode, err := z.zk.GetPartState(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//检查该Partition的状态是否设定
	//检查该Partition在Brokers上是否创建raft集群或fetch
	Brokers := make(map[string]string)
	var ret string
	Dups := z.zk.GetDuplicateNodes(block.TopicName, block.PartitionName, block.Name)
	for _, DupNode := range Dups {
		BrokerNode, err := z.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		Brokers[DupNode.BrokerName] = BrokerNode.BrokHostPort
	}

	data, err := json.Marshal(Brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	for BrokerName, BrokerHostPort := range Brokers {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[BrokerName]
		z.mu.RUnlock()

		//未连接该broker
		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(BrokerHostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
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
			logger.DEBUG(logger.DError, err.Error()+resp.Err)
		}

		//检查该Partition的状态是否设定
		//检查该Partition在Brokers上是否创建raft集群或fetch
		//若该Partition没有设置状态则返回通知producer
		if PartitionNode.Option == -2 { //未设置状态
			ret = "Partition State is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName: block.TopicName,
				PartName:  block.PartitionName,
				State:     PartitionNode.Option,
				Brokers:   data,
			})
			if err != nil || !resp.Ret {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
	}

	//返回producer broker的host_port
	return Info_out{
		Err:           err,
		broker_name:   broker.Name,
		bro_host_port: broker.BrokHostPort,
		Ret:           ret,
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
		Name:      info.part_name,
		Index:     int64(1),
		TopicName: info.topic_name,
		Option:    -2,
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
func (z *ZkServer) SetPartitionState(info Info_in) Info_out {
	var ret string
	var data_brokers []byte
	var Dups []zookeeper.DuplicateNode
	node, err := z.zk.GetPartState(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{
			Err: err,
		}
	}

	if info.option != node.Option {
		index, err := z.zk.GetPartBlockIndex(info.topic_name, info.part_name)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return Info_out{
				Err: err,
			}
		}
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topic_name,
			Name:      info.part_name,
			Index:     index,
			Option:    info.option,
			PTPoffset: node.PTPoffset,
			DupNum:    info.dupnum, //需要下面的程序确认，是否能分配一定数量的副本
		})
	}

	logger.DEBUG(logger.DLog, "this partition(%v) status is %v\n", node.Name, node.Option)

	if node.Option == -2 {
		//未创建任何状态, 即该partition未接收过任何信息

		switch info.option {
		case -1:
			//负载均衡获得一定数量broker节点,并在这些broker上部署raft集群
			// raft副本个数暂时默认3个

			Dups, data_brokers = z.GetDupsFromConsist(info)

			//向这些broker发送信息，启动raft
			for _, dupnode := range Dups {
				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//开启raft集群
					resp, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						Brokers:   data_brokers,
					})

					logger.DEBUG(logger.DLog, "the broker %v had add raft\n", dupnode.BrokerName)
					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp, err.Error())
						return Info_out{
							Err: err,
						}
					}
				}
			}
		default:
			//负载均衡获得一定数量broker节点,选择一个leader, 并让其他节点fetch leader信息
			//默认副本数为3

			Dups, data_brokers = z.GetDupsFromConsist(info)

			LeaderBroker, err := z.zk.GetBrokerNode(Dups[0].BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{
					Err: err,
				}
			}

			for _, dupnode := range Dups {
				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//开启fetch机制
					resp3, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp3, err.Error())
						return Info_out{
							Err: err,
						}
					}
				}
			}
		}

		return Info_out{
			Ret: ret,
		}
	}

	//获取该partition
	LeaderBroker, NowBlock, _, err := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{
			Err: err,
		}
	}
	Dups = z.zk.GetDuplicateNodes(NowBlock.TopicName, NowBlock.PartitionName, NowBlock.Name)

	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RafBrokers = make(map[string]string)
	for _, DupNode := range Dups {
		BrokerNode, err := z.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		brokers.BroBrokers[DupNode.BrokerName] = BrokerNode.BrokHostPort
		brokers.RafBrokers[DupNode.BrokerName] = BrokerNode.RaftHostPort
	}

	data_brokers, err = json.Marshal(brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{
			Err: err,
		}
	}

	switch info.option {
	case -1:
		if node.Option == -1 { //与原来的状态相同
			ret = "HadRaft"
		}
		if node.Option == 1 || node.Option == 0 { //原状态为fetch, 关闭原来的写状态, 创建新raft集群
			//查询raft集群的broker, 发送信息
			//fetch操作继续同步之前文件, 创建raft集群,需要更换新文件写入
			//调用CloseAcceptPartition更换文件
			for ice, dupnode := range Dups {
				//停止接收该partition的信息，更换一个新文件写入信息，因为fetch机制一些信息已经写入leader
				//但未写入follower中，更换文件从头写入，重新开启fetch机制为上一个文件同步信息
				lastfilename := z.CloseAcceptPartition(info.topic_name, info.part_name, dupnode.BrokerName, ice)

				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					// ret := "this partition leader broker is not connected"
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.part_name)
				} else {
					//关闭fetch机制
					resp1, err := bro_cli.CloseFetchPartition(context.Background(), &api.CloseFetchPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
					})
					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp1, err.Error())
						return Info_out{
							Err: err,
						}
					}

					//重新准备接收文件
					resp2, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						FileName:  "NowBlock.txt",
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp2, err.Error())
						return Info_out{
							Err: err,
						}
					}

					//开启raft集群
					resp3, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
						Brokers:   data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp3, err.Error())
						return Info_out{
							Err: err,
						}
					}

					//开启fetch机制,同步完上一个文件
					resp4, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     lastfilename,
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp4, err.Error())
						return Info_out{
							Err: err,
						}
					}
				}

			}

		}

	default:
		if node.Option != -1 { //与原状态相同
			ret = "HadFetch"
		} else { //由raft改为fetch
			//查询fetch的Broker, 发送信息
			// 关闭raft集群, 开启fetch操作,不需要更换文件

			for _, dupnode := range Dups {
				//停止接收该partition的信息，当raft被终止后broker server将不会接收该partition的信息
				//NowBlock的文件不需要关闭接收信息，启动fetch机制后，可以继续向该文件写入信息
				// z.CloseAcceptPartition(info.topic_name, info.part_name, dupnode.BrokerName)

				bro_cli, ok := z.Brokers[dupnode.BrokerName]
				if !ok {
					// ret := "this partition leader broker is not connected"
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.part_name)
					// bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(LeaderBroker.HostPort))
					// if err != nil {
					// 	logger.DEBUG(logger.DError, "%v\n", err.Error())
					// }
					// z.mu.Lock()
					// z.Brokers[dupnode.BrokerName] = bro_cli
					// z.mu.Unlock()
				} else {
					//关闭raft集群
					resp1, err := bro_cli.CloseRaftPartition(context.Background(), &api.CloseRaftPartitionRequest{
						TopicName: info.topic_name,
						PartName:  info.part_name,
					})
					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp1, err.Error())
						return Info_out{
							Err: err,
						}
					}

					//开启fetch机制
					resp2, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topic_name,
						PartName:     info.part_name,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v  err(%v)\n", resp2, err.Error())
						return Info_out{
							Err: err,
						}
					}
				}

			}
		}
	}
	return Info_out{
		Ret: ret,
	}
}

func (z *ZkServer) GetDupsFromConsist(info Info_in) (Dups []zookeeper.DuplicateNode, data_brokers []byte) {
	str := info.topic_name + info.part_name
	Bro_dups := z.consistent.GetNode(str+"dup", 3)
	// Bro_dup_2 := z.consistent.GetNode(str + "dup2")
	// Bro_dup_3 := z.consistent.GetNode(str + "dup3")
	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:          "dup_0",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		BrokerName:    Bro_dups[0],
		StartOffset:   int64(0),
		BlockName:     "NowBlock",
	})

	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:          "dup_1",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		BrokerName:    Bro_dups[1],
		StartOffset:   int64(0),
		BlockName:     "NowBlock",
	})

	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:          "dup_2",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		BrokerName:    Bro_dups[2],
		StartOffset:   int64(0),
		BlockName:     "NowBlock",
	})

	for _, dup := range Dups {
		err := z.zk.RegisterNode(dup)
		if err != nil {
			logger.DEBUG(logger.DError, "the err is %v\n", err.Error())
		}
	}

	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RafBrokers = make(map[string]string)
	brokers.Me_Brokers = make(map[string]int)
	for _, DupNode := range Dups {
		BrokerNode, err := z.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		brokers.BroBrokers[DupNode.BrokerName] = BrokerNode.BrokHostPort
		brokers.RafBrokers[DupNode.BrokerName] = BrokerNode.RaftHostPort
		brokers.Me_Brokers[DupNode.BrokerName] = BrokerNode.Me
	}

	data_brokers, err := json.Marshal(brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	return Dups, data_brokers
}

func (z *ZkServer) CreateNowBlock(info Info_in) error {
	block_node := zookeeper.BlockNode{
		Name:          "NowBlock",
		FileName:      info.topic_name + info.part_name + "now.txt",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		StartOffset:   int64(0),
	}
	return z.zk.RegisterNode(block_node)
}

func (z *ZkServer) BecomeLeader(info Info_in) error {
	logger.DEBUG(logger.DLeader, "partition(%v) new leader is %v\n", info.topic_name+info.part_name, info.cli_name)
	now_block_path := z.zk.TopicRoot + "/" + info.topic_name + "/" + "Partitions" + "/" + info.part_name + "/" + "NowBlock"
	NowBlock, err := z.zk.GetBlockNode(now_block_path)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	NowBlock.LeaderBroker = info.cli_name
	return z.zk.UpdateBlockNode(NowBlock)
}

func (z *ZkServer) SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中

	return nil
}

//consumer查询该向那些broker发送请求
//zkserver让broker准备好topic/sub和config
func (z *ZkServer) HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var Parts []zookeeper.Part

	// 检查该应户是否订阅了该topic/partition
	z.zk.CheckSub(zookeeper.StartGetInfo{
		Cli_name:      info.cli_name,
		Topic_name:    info.topic_name,
		PartitionName: info.part_name,
		Option:        info.option,
	})

	//获取该topic或partition的broker,并保证在线,若全部离线则Err
	if info.option == TOPIC_NIL_PTP_PULL || info.option == TOPIC_NIL_PTP_PUSH { //ptp_push
		Parts, err = z.zk.GetBrokers(info.topic_name)
	} else if info.option == TOPIC_KEY_PSB_PULL || info.option == TOPIC_KEY_PSB_PUSH { //psb_push
		Parts, err = z.zk.GetBroker(info.topic_name, info.part_name, info.index)
	}
	if err != nil {
		return nil, 0, err
	}

	logger.DEBUG(logger.DLog, "the brokers is %v\n", Parts)

	//获取到信息后将通知brokers，让他们检查是否有该Topic/Partition/Subscription/config等
	//并开启Part发送协程，若协程在超时时间到后未收到管道的信息，则关闭该协程
	// var partkeys []clients.PartKey
	// if info.option == 1 || info.option == 3 {
	partkeys := z.SendPreoare(Parts, info)
	// }else{
	// partkeys = GetPartKeys(Parts)
	// }

	parts := clients.Parts{
		PartKeys: partkeys,
	}
	data, err := json.Marshal(parts)

	var nodes clients.Parts

	json.Unmarshal(data, &nodes)

	logger.DEBUG(logger.DLog, "the partkes %v and nodes is %v\n", partkeys, nodes)

	if err != nil {
		logger.DEBUG(logger.DError, "turn partkeys to json faile %v", err.Error())
	}

	return data, size, nil
}


func (z *ZkServer) SendPreoare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey) {

	for _, part := range Parts {
		if part.Err != OK { 
			logger.DEBUG(logger.DLog, "the part.ERR(%v) != OK the part is %v\n", part.Err, part)
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(part.BrokHost_Port))
			if err != nil {
				logger.DEBUG(logger.DError, "broker(%v) host_port(%v) con't connect %v", part.BrokerName, part.BrokHost_Port, err.Error())
			}
			z.mu.Lock()
			z.Brokers[part.BrokerName] = bro_cli
			z.mu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			Consumer: info.cli_name,
			TopicName: info.topic_name,
			PartName:  part.Part_name,
			FileName:  part.File_name,
			Option:    info.option,
		}
		if rep.Option == TOPIC_NIL_PTP_PULL || rep.Option == TOPIC_NIL_PTP_PUSH { //ptp
			rep.Offset = part.PTP_index
		} else if rep.Option == TOPIC_KEY_PSB_PULL || rep.Option == TOPIC_KEY_PSB_PUSH { //psb
			rep.Offset = info.index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "PrepareSend error %v", err.Error())
		}
		logger.DEBUG(logger.DLog, "the part is %v\n", part)
		partkeys = append(partkeys, clients.PartKey{
			Name:        part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P:  part.BrokHost_Port,
			Offset:      part.PTP_index,
			Err:         OK,
		})
	}

	return partkeys
}

//修改Offset
func (z *ZkServer) UpdatePTPOffset(info Info_in) error {
	str := z.zk.TopicRoot + "/" + info.topic_name + "/" + "Partitions" + "/" + info.part_name
	node, err := z.zk.GetPartitionNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}

	err = z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name:      info.part_name,
		TopicName: info.topic_name,
		Index:     node.Index,
		Option:    node.Option,
		DupNum:    node.DupNum,
		PTPoffset: info.index,
	})
	return err
}

func (z *ZkServer) UpdateDupNode(info Info_in) error {
	str := z.zk.TopicRoot + "/" + info.topic_name + "/" + "Partitions" + "/" + info.part_name + "/" + info.blockname
	// if info.leader {
	BlockNode, err := z.zk.GetBlockNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	if info.index > BlockNode.EndOffset {
		BlockNode.EndOffset = info.index
		err = z.zk.RegisterNode(BlockNode)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return err
		}
	}
	// }
	DupNode, err := z.zk.GetDuplicateNode(str + "/" + info.cli_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	DupNode.EndOffset = info.index
	err = z.zk.RegisterNode(DupNode)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	return nil
}

func GetPartKeys(Parts []zookeeper.Part) (partkeys []clients.PartKey) {
	for _, part := range Parts {
		partkeys = append(partkeys, clients.PartKey{
			Name:        part.Part_name,
			Broker_name: part.BrokerName,
			Broker_H_P:  part.BrokHost_Port,
		})
	}
	return partkeys
}

//需要向每个副本都发送
//发送请求到broker，关闭该broker上的partition的接收程序
//并修改NowBlock的文件名，并修改zookeeper上的block信息
func (z *ZkServer) CloseAcceptPartition(topicname, partname, brokername string, ice int) string {

	//获取新文件名
	index, err := z.zk.GetPartBlockIndex(topicname, partname)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error()
	}
	NewBlockName := "Block_" + strconv.Itoa(int(index))
	NewFileName := NewBlockName + ".txt"

	z.mu.RLock()
	bro_cli, ok := z.Brokers[brokername]
	if !ok {
		logger.DEBUG(logger.DError, "broker(%v) is not connected\n", brokername)
		// bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(LeaderBroker.HostPort))
		// if err != nil {
		// 	logger.DEBUG(logger.DError, "%v\n", err.Error())
		// }
		// z.mu.Lock()
		// z.Brokers[brokername] = bro_cli
		// z.mu.Unlock()
	} else {
		resp, err := bro_cli.CloseAccept(context.Background(), &api.CloseAcceptRequest{
			TopicName:    topicname,
			PartName:     partname,
			Oldfilename:  "NowBlock.txt",
			Newfilename_: NewFileName,
		})
		if err != nil && !resp.Ret {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		} else {
			str := z.zk.TopicRoot + "/" + topicname + "/Partitions/" + partname + "/" + "NowBlock"
			bnode, err := z.zk.GetBlockNode(str)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}

			if ice == 0 {
				//创建新节点
				z.zk.RegisterNode(zookeeper.BlockNode{
					Name:          NewBlockName,
					TopicName:     topicname,
					PartitionName: partname,
					FileName:      NewFileName,
					StartOffset:   resp.Startindex,
					EndOffset:     resp.Endindex,

					LeaderBroker: bnode.LeaderBroker,
				})

				//更新原NowBlock节点信息
				z.zk.UpdateBlockNode(zookeeper.BlockNode{
					Name:          "NowBlock",
					TopicName:     topicname,
					PartitionName: partname,
					FileName:      "NowBlock.txt",
					StartOffset:   resp.Endindex + 1,
					//leader暂时未选出
				})
			}

			//创建该节点下的各个Dup节点
			DupPath := z.zk.TopicRoot + "/" + topicname + "/Partitions/" + partname + "/" + "NowBlock" + "/" + brokername
			DupNode, err := z.zk.GetDuplicateNode(DupPath)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}

			DupNode.BlockName = NewBlockName

			z.zk.RegisterNode(DupNode) //在NewBlock上创建副本节点
		}
	}
	z.mu.RUnlock()

	return NewFileName
}

func (z *ZkServer) GetNewLeader(info Info_in) (Info_out, error) {
	block_path := z.zk.TopicRoot + "/" + info.topic_name + "/" + "Partitions" + "/" + info.part_name + "/" + info.blockname

	BlockNode, err := z.zk.GetBlockNode(block_path)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	var LeaderBroker zookeeper.BrokerNode
	//需要检查Leader是否在线，若不在线需要更换leader
	// broker_path := z.zk.BrokerRoot + "/" + BlockNode.LeaderBroker
	ret := z.zk.CheckBroker(BlockNode.LeaderBroker)
	if ret {
		LeaderBroker, err = z.zk.GetBrokerNode(BlockNode.LeaderBroker)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	} else {
		//检查副本中谁的最新，再次检查
		var array []struct {
			EndIndex   int64
			BrokerName string
		}
		Dups := z.zk.GetDuplicateNodes(info.topic_name, info.part_name, info.blockname)
		for _, dup := range Dups {
			// str := z.zk.BrokerRoot + "/" + dup.BrokerName
			ret = z.zk.CheckBroker(dup.BrokerName)
			if ret {
				//根据EndIndex的大小排序
				array = append(array, struct {
					EndIndex   int64
					BrokerName string
				}{dup.EndOffset, dup.BrokerName})
			}
		}

		sort.SliceStable(array, func(i, j int) bool {
			return array[i].EndIndex > array[j].EndIndex
		})

		for _, arr := range array {
			LeaderBroker, err = z.zk.GetBrokerNode(arr.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			ret = z.zk.CheckBroker(arr.BrokerName)
			if ret {
				break
			}
		}
	}

	return Info_out{
		broker_name:   LeaderBroker.Name,
		bro_host_port: LeaderBroker.BrokHostPort,
	}, nil
}

type ConsistentBro struct {
	// 排序的hash虚拟节点（环形）
	hashSortedNodes []uint32
	// 虚拟节点(broker)对应的实际节点
	circle map[uint32]string
	// 已绑定的broker为true
	nodes map[string]bool

	BroH map[string]bool

	mu sync.RWMutex
	//虚拟节点个数
	vertualNodeCount int
}

func NewConsistentBro() *ConsistentBro {
	con := &ConsistentBro{
		hashSortedNodes: make([]uint32, 2),
		circle:          make(map[uint32]string),
		nodes:           make(map[string]bool),
		BroH:            make(map[string]bool),

		mu:               sync.RWMutex{},
		vertualNodeCount: VERTUAL_10,
	}

	return con
}

func (c *ConsistentBro) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add consumer name as node
func (c *ConsistentBro) Add(node string, power int) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; ok {
		// fmt.Println("node already existed")
		return errors.New("node already existed")
	}
	c.nodes[node] = true

	for i := 0; i < c.vertualNodeCount*power; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *ConsistentBro) Reduce(node string) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; !ok {
		// fmt.Println("node already existed")
		return errors.New("node already delete")
	}
	// c.nodes[node] = false
	delete(c.nodes, node)

	for i := 0; i < c.vertualNodeCount; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		delete(c.circle, virtualKey)
		for j := 0; j < len(c.hashSortedNodes); j++ {
			if c.hashSortedNodes[j] == virtualKey && j != len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = append(c.hashSortedNodes[:j], c.hashSortedNodes[j+1:]...)
			} else if j == len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = c.hashSortedNodes[:j]
			}
		}
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *ConsistentBro) SetBroHFalse() {
	for Bro := range c.BroH {
		c.BroH[Bro] = false
	}
}

// return consumer name
func (c *ConsistentBro) GetNode(key string, num int) (dups []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SetBroHFalse()

	hash := c.hashKey(key)
	for index := 0; index < num; index++ {
		i := c.getPosition(hash)

		broker_name := c.circle[c.hashSortedNodes[i]]

		dups = append(dups, broker_name)

		c.BroH[broker_name] = true
	}

	return dups
}

func (c *ConsistentBro) getPosition(hash uint32) (ret int) {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })

	if i < len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			ret = 0
		} else {
			ret = i
		}
	} else {
		ret = len(c.hashSortedNodes) - 1
	}

	for c.BroH[c.circle[c.hashSortedNodes[ret]]] {
		ret++
	}

	return ret
}
