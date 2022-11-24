package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/raft_operations"
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
	Name      string
	topics    map[string]*Topic
	consumers map[string]*Client
	zk        *zookeeper.ZK
	zkclient  zkserver_operations.Client
	mu        sync.RWMutex

	aplych  chan info
	brokers map[string]*raft_operations.Client

	//raft
	parts_rafts *parts_raft
}

type Key struct {
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
	Size        int   `json:"size"`
}

type Message struct {
	Index      int64  `json:"index"`
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
	message  string

	//raft
	brokers map[string]string
	me      int
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
	name = GetIpport()
	s.CheckList()
	s.Name = opt.Name

	//本地创建parts——raft，为raft同步做准备
	s.parts_rafts = NewParts_Raft()
	go s.parts_rafts.make(opt.Name, opt.Raft_Host_Port, s.aplych)

	//在zookeeper上创建一个永久节点, 若存在则不需要创建
	err := s.zk.RegisterNode(zookeeper.BrokerNode{
		Name: s.Name,
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
func (s *Server) GetApplych(applych chan info){

	for msg := range applych {
		s.mu.RLock()
		topic, ok := s.topics[msg.topic_name]
		s.mu.RUnlock()

		if !ok {
			DEBUG(dError, "topic(%v) is not in this broker\n", msg.topic_name)
		}else{
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
	//检查或创建partition
	return topic.PrepareAcceptHandle(in)
}

func (s *Server) CloseAcceptHandle(in info) {

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
func (s *Server) PushHandle(in info) (ret string, err error ){

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
	case -1:		//raft同步,并写入
		ret, err = part_raft.Append(in)
	case 1:			//leader写入,不等待同步
		err = topic.addMessage(in)
	case 0:			//直接返回
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
		读取index，获得上次的index，写入zookeeper中
	*/
	s.zkclient.UpdateOffset(context.Background(), &api.UpdateOffsetRequest{
		Topic: in.topic_name,
		Part: in.part_name,
		Offset: in.offset,
	})

	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	s.mu.RUnlock()
	if !ok {
		DEBUG(dError, "this topic is not in this broker\n")
		return MSGS{}, errors.New("this topic is not in this broker")
	}

	return topic.PullMessage(in)
}
