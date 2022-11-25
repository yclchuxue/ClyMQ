package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/raft_operations"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"ClyMQ/zookeeper"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/cloudwego/kitex/client"
)

var (
	name string
)

const (
	NODE_SIZE = 42
)

type Server struct {
	Name     string
	zk       *zookeeper.ZK
	zkclient zkserver_operations.Client
	mu       sync.RWMutex

	aplych    chan info
	topics    map[string]*Topic
	consumers map[string]*Client
	brokers   map[string]*raft_operations.Client

	//raft
	parts_rafts *parts_raft

	//fetch
	parts_fetch   map[string]string                    //topicName + partitionName to broker HostPort
	brokers_fetch map[string]*server_operations.Client //brokerName to Client
}

type Key struct {
	Size        int   `json:"size"`
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
}

type Message struct {
	Index      int64  `json:"index"`
	Size 	   int8   `json:"size"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

// type Msg struct {
// 	producer string
// 	topic    string
// 	key      string
// 	msg      []byte
// }

type info struct {
	// name       string //broker name
	topic_name string
	part_name  string
	file_name  string
	new_name   string
	option     int8
	offset     int64
	size       int8

	ack int8

	producer string
	consumer string
	cmdindex int64
	// startIndex  int64
	// endIndex  	int64
	message  []byte

	//raft
	brokers map[string]string
	me      int

	//fetch
	LeaderBroker string
	HostPort     string
}

// type startget struct {
// 	cli_name   string
// 	topic_name string
// 	part_name  string
// 	index      int64
// 	option     int8
// }

func NewServer(zkinfo zookeeper.ZkInfo) *Server {
	return &Server{
		zk: zookeeper.NewZK(zkinfo), //连接上zookeeper
		mu: sync.RWMutex{},
	}
}

func (s *Server) make(opt Options) {

	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)
	s.brokers = make(map[string]*raft_operations.Client)
	s.parts_fetch = make(map[string]string)
	s.brokers_fetch = make(map[string]*server_operations.Client)
	s.aplych = make(chan info)

	s.CheckList()
	s.Name = opt.Name

	//本地创建parts——raft，为raft同步做准备
	s.parts_rafts = NewParts_Raft()
	go s.parts_rafts.make(opt.Name, opt.Raft_Host_Port, s.aplych)

	//在zookeeper上创建一个永久节点, 若存在则不需要创建
	err := s.zk.RegisterNode(zookeeper.BrokerNode{
		Name:     s.Name,
		HostPort: opt.Broker_Host_Port,
	})
	if err != nil {
		DEBUG(dError, err.Error())
	}
	//创建临时节点,用于zkserver的watch
	err = s.zk.CreateState(s.Name)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	//连接zkServer，并将自己的Info发送到zkServer,
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.Zkserver_Host_Port))
	if err != nil {
		DEBUG(dError, err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName:     opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error())
	}
	// s.IntiBroker() 根据zookeeper上的历史信息，加载缓存信息
}

//接收applych管道的内容
//写入partition文件中
func (s *Server) GetApplych(applych chan info) {

	for msg := range applych {
		s.mu.RLock()
		topic, ok := s.topics[msg.topic_name]
		s.mu.RUnlock()

		if !ok {
			DEBUG(dError, "topic(%v) is not in this broker\n", msg.topic_name)
		} else {
			topic.addMessage(msg)
		}
	}
}

// IntiBroker
//not used
//获取该Broker需要负责的Topic和Partition,并在本地创建对应配置
func (s *Server) IntiBroker() {
	s.mu.Lock()
	info := Property{
		Name:  s.Name,
		Power: 1,
		//获取Broker性能指标
	}
	data, err := json.Marshal(info)
	if err != nil {
		DEBUG(dError, err.Error())
	}

	resp, err := s.zkclient.BroGetConfig(context.Background(), &api.BroGetConfigRequest{
		Propertyinfo: data,
	})

	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error())
	}
	BroInfo := BroNodeInfo{
		Topics: make(map[string]TopNodeInfo),
	}
	json.Unmarshal(resp.Brokerinfo, &BroInfo)

	s.HandleTopics(BroInfo.Topics)

	s.mu.Unlock()
}

// HandleTopics not used
func (s *Server) HandleTopics(Topics map[string]TopNodeInfo) {

	for topic_name, topic := range Topics {
		_, ok := s.topics[topic_name]
		if !ok {
			top := NewTopic(topic_name)
			top.HandleParttitions(topic.Partitions)

			s.topics[topic_name] = top
		} else {
			DEBUG(dWarn, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}

func (s *Server) CheckList() {
	str, _ := os.Getwd()
	str += "/" + name
	ret := CheckFileOrList(str)
	// DEBUG(dLog, "Check list %v is %v\n", str, ret)
	if !ret {
		CreateList(str)
	}
}

const (
	ErrHadStart = "this partition had Start"
)

// PrepareAcceptHandle
//准备接收信息，
//检查topic和partition是否存在，不存在则需要创建，
//设置partition中的file和fd，start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {
	//检查或创建完topic
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(in.topic_name)
		s.topics[in.topic_name] = topic
	}

	s.mu.Unlock()
	//检查或创建partition
	return topic.PrepareAcceptHandle(in)
}

//停止接收文件，并将文件名修改成newfilename
func (s *Server) CloseAcceptHandle(in info) (ret string, err error) {
	s.mu.RLock()

	s.mu.RUnlock()
}

// PrepareSendHandle
//准备发送信息，
//检查topic和subscription是否存在，不存在则需要创建
//检查该文件的config是否存在，不存在则创建，并开启协程
//协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in info) (ret string, err error) {
	//检查或创建topic
	s.mu.Lock()
	topic, ok := s.topics[in.topic_name]
	if !ok {
		topic = NewTopic(in.topic_name)
		s.topics[in.topic_name] = topic
	}
	s.mu.Unlock()
	//检查或创建partition
	return topic.PrepareSendHandle(in, &s.zkclient)
}

func (s Server) AddRaftHandle(in info) (ret string, err error) {
	//检测该Partition的Raft是否已经启动

	s.mu.Lock()
	var peers []*raft_operations.Client
	for k, v := range in.brokers {
		if k != s.Name {
			bro_cli, ok := s.brokers[k]
			if !ok {
				cli, err := raft_operations.NewClient(s.Name, client.WithHostPorts(v))
				if err != nil {
					return ret, err
				}
				s.brokers[k] = &cli
				bro_cli = &cli
			}
			peers = append(peers, bro_cli)
		}
	}

	//检查或创建底层part_raft
	s.parts_rafts.AddPart_Raft(peers, in.me, in.topic_name, in.part_name, s.aplych)

	s.mu.Unlock()

	return ret, err
}

func (s *Server) CloseRaftHandle(in info) (ret string, err error) {

}

func (s *Server) AddFetchHandle(in info) (ret string, err error) {
	//检测该Partition的fetch机制是否已经启动

	if in.LeaderBroker == s.Name {
		//Leader Broker将准备好接收follower的Pull请求
		s.mu.RLock()
		defer s.mu.RUnlock()
		topic, ok := s.topics[in.topic_name]
		if !ok {
			ret = "this topic is not in this broker"
			DEBUG(dError, "%v, info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		//给每个follower broker准备node（PSB_PULL），等待pull请求
		for BrokerName, _ := range in.brokers {
			ret, err = topic.PrepareSendHandle(info{
				topic_name: in.topic_name,
				part_name:  in.part_name,
				file_name:  in.file_name,
				consumer:   BrokerName,
				option:     4, //PSB_PULL
			}, &s.zkclient)
			if err != nil {
				DEBUG(dError, err.Error())
			}
		}
		return ret, err
	} else {
		str := in.topic_name + in.part_name
		s.mu.Lock()
		broker, ok := s.brokers_fetch[in.LeaderBroker]
		if !ok {
			broker, err := server_operations.NewClient(s.Name, client.WithHostPorts(in.HostPort))
			if err != nil {
				DEBUG(dError, err.Error())
				return err.Error(), err
			}
			s.brokers_fetch[in.LeaderBroker] = &broker
		}

		_, ok = s.parts_fetch[str]
		if !ok {
			s.parts_fetch[str] = in.LeaderBroker
		}
		topic, ok := s.topics[in.topic_name]
		if !ok {
			ret = "this topic is not in this broker"
			DEBUG(dError, "%v, info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		s.mu.Unlock()

		return s.FetchMsg(in, broker, topic)
	}
}

func (s *Server) CloseFetchHandle(in info) (ret string, err error) {

}

func (s *Server) InfoHandle(ipport string) error {

	DEBUG(dLog, "get consumer's ip_port %v\n", ipport)

	client, err := client_operations.NewClient("client", client.WithHostPorts(ipport))
	if err == nil {
		DEBUG(dLog, "connect consumer server successful\n")
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		s.mu.Unlock()
		DEBUG(dLog, "return resp to consumer\n")
		return nil
	}

	DEBUG(dError, "Connect client failed")

	return err
}

func (s *Server) StartGet(in info) (err error) {
	/*
		新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

		查询是否有该订阅的信息；

		PTP：需要负载均衡

		PSB：不需要负载均衡,每个PSB开一个Part来发送信息
	*/
	err = nil
	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()
		//已经由zkserver检查过是否订阅
		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)
		//添加到Config后会进行负载均衡，生成新的配置，然后执行新的配置
		return s.topics[in.topic_name].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())

	case TOPIC_KEY_PSB_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(in.topic_name, in.part_name, in.option)

		DEBUG(dLog, "consumer(%v) start to get topic(%v) partition(%v) offset(%v) in sub(%v)\n", in.consumer, in.topic_name, in.part_name, in.offset, sub_name)

		return s.topics[in.topic_name].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())
	default:
		err = errors.New("the option is not PTP or PSB")
	}

	return err
}

func (s *Server) CheckConsumer(client *Client) {
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList {
			subscription.ShutdownConsumerInGroup(client.name)
		}
		client.mu.Unlock()
	}
}

// SubHandle
// subscribe 订阅
func (s *Server) SubHandle(in info) (err error) {
	s.mu.Lock()
	DEBUG(dLog, "get sub information\n")
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(in)
	if err != nil {
		s.consumers[in.consumer].AddSubScription(sub)
	}
	// resp.parts = GetPartKeyArray(s.topics[req.topic].GetParts())
	// resp.size = len(resp.parts)
	s.mu.Unlock()

	return nil
}

func (s *Server) UnSubHandle(in info) error {

	s.mu.Lock()
	DEBUG(dLog, "get unsub information\n")
	top, ok := s.topics[in.topic_name]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub_name, err := top.ReduceSubScription(in)
	if err != nil {
		s.consumers[in.consumer].ReduceSubScription(sub_name)
	}

	s.mu.Unlock()
	return nil
}

//start到该partition中的raft集群中
//收到返回后判断该写入还是返回
func (s *Server) PushHandle(in info) (ret string, err error) {

	DEBUG(dLog, "get Message form producer\n")
	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	part_raft := s.parts_rafts
	s.mu.RUnlock()

	if !ok {
		ret = "this topic is not in this broker"
		DEBUG(dError, "Topic %v, is not in this broker\n", in.topic_name)
		return ret, errors.New(ret)
	}

	switch in.ack {
	case -1: //raft同步,并写入
		ret, err = part_raft.Append(in)
	case 1: //leader写入,不等待同步
		err = topic.addMessage(in)
	case 0: //直接返回
		go topic.addMessage(in)
	}

	if err != nil {
		DEBUG(dError, err.Error())
		return err.Error(), err
	}

	return ret, err
}

// PullHandle
// Pull message
func (s *Server) PullHandle(in info) (MSGS, error) {

	/*
		若该请求属于PTP则
		读取index，获得上次的index，写入zookeeper中
	*/
	if in.option == TOPIC_NIL_PTP_PULL {
		s.zkclient.UpdateOffset(context.Background(), &api.UpdateOffsetRequest{
			Topic:  in.topic_name,
			Part:   in.part_name,
			Offset: in.offset,
		})
	}

	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	s.mu.RUnlock()
	if !ok {
		DEBUG(dError, "this topic is not in this broker\n")
		return MSGS{}, errors.New("this topic is not in this broker")
	}

	return topic.PullMessage(in)
}

func (s *Server) FetchMsg(in info, client *server_operations.Client, topic *Topic) (ret string, err error) {
	//向zkserver请求向Leader Broker Pull信息

	//向LeaderBroker发起Pull请求
	//获得本地本地当前文件end_index
	File := topic.GetFile(in)
	fd := File.OpenFile()
	index := File.GetIndex(fd)
	index += 1
	// _, err := File.FindOffset(fd, index)
	if err != nil {
		DEBUG(dError, err.Error())
		return err.Error(), err
	}

	if in.file_name != "Nowfile.txt" {
		//当文件名不为nowfile时
		//创建一partition, 并向该File中写入内容

		go func() {

			Partition := NewPartition(in.topic_name, in.part_name)
			Partition.StartGetMessage(File, fd, in)

			for {
				resp, err := (*client).Pull(context.Background(), &api.PullRequest{
					Consumer: s.Name,
					Topic:    in.topic_name,
					Key:      in.part_name,
					Offset:   index,
				})

				if err != nil {
					DEBUG(dError, "Err %v, err(%v)\n", resp.Err, err.Error())
				}
				
				if resp.StartIndex <= index && resp.EndIndex > index {
					//index 处于返回包的中间位置
					//需要截断该宝包，并写入
					//your code
					DEBUG(dLog, "need your code\n")
				}

				node := Key{
					Start_index: resp.StartIndex,
					End_index:   resp.EndIndex,
					Size:        len(resp.Msgs),
				}

				File.WriteFile(fd, node, resp.Msgs)
				index = resp.EndIndex + 1
			}

		}()

	} else {
		//当文件名为nowfile时
		//zkserver已经让该broker准备接收文件
		//直接调用addMessage

		go func() {
			s.mu.RLock()
			topic, ok := s.topics[in.topic_name]
			s.mu.RUnlock()
			if !ok {
				DEBUG(dError, err.Error())
			}

			resp, err := (*client).Pull(context.Background(), &api.PullRequest{
				Consumer: s.Name,
				Topic:    in.topic_name,
				Key:      in.part_name,
				Offset:   index,
			})

			if err != nil {
				DEBUG(dError, "Err %v, err(%v)\n", resp.Err, err.Error())
			}

			msgs := make([]Message, resp.Size)
			json.Unmarshal(resp.Msgs, &msgs)

			start_index := resp.StartIndex
			for _, msg := range msgs {
				
				if index == start_index {
					err := topic.addMessage(info{
						topic_name: in.topic_name,
						part_name:  in.part_name,
						size:       msg.Size,
						message:    msg.Msg,
					})
					if err != nil {
						DEBUG(dError, err.Error())
					}
				}
				index++
			}

		}()

	}
	return ret, err
}
