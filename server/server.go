package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"ClyMQ/zookeeper"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/cloudwego/kitex/client"
	client2 "github.com/cloudwego/kitex/client"
)

var (
	name string
)

const (
	NODE_SIZE  =  42
)

type Server struct {
	Name  		string
	topics 		map[string]*Topic
	consumers 	map[string]*Client
	zk 			zookeeper.ZK
	zkclient 	zkserver_operations.Client
	mu 			sync.RWMutex
}

type Key struct {
	Start_index int64	`json:"start_index"`
	End_index 	int64	`json:"end_index"`
	Size  		int 	`json:"size"`
}

type Message struct {
	Index 		int64 		`json:"index"`
	Topic_name 	string		`json:"topic_name"`
	Part_name 	string		`json:"part_name"`
	Msg 		[]byte		`json:"msg"`
}

type Msg struct {
	producer string
	topic    string
	key      string
	msg 	 []byte
}

type push struct {
	producer string
	topic    string
	key      string
	message  string
}

type info struct{
	name		string  //broker name
	topic_name 	string
	part_name	string
	file_name	string
	option 		int8
	offset 		int64

	producer 	string
	consumer 	string
}

type pull struct {
	consumer string
	topic    string
	key      string
}

type retpull struct{
	message string
}

type sub struct{
	consumer string
	topic string
	key   string
	option int8
}

// type retsub struct{
// 	size int
// 	parts []PartKey
// }

type startget struct{
	cli_name  	string
    topic_name	string
    part_name	string
    index		int64
	option 		int8
}

func NewServer(zkinfo zookeeper.ZkInfo) *Server {
	return &Server{
		zk: *zookeeper.NewZK(zkinfo),  //连接上zookeeper
		mu:sync.RWMutex{},
	}
}

func (s *Server) make(opt Options) {

	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)
	name = GetIpport()
	s.CheckList()
	s.Name = opt.Name

	//连接zkServer，并将自己的Info发送到zkServer,
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.Zkserver_Host_Port))
	if err != nil {
		DEBUG(dError, err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName: opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		DEBUG(dError, err.Error())
	}
	// s.IntiBroker() 根据zookeeper上的历史信息，加载缓存信息
}

//获取该Broker需要负责的Topic和Partition,并在本地创建对应配置
func (s *Server)IntiBroker(){	
	s.mu.Lock()
	info := Property{
		Name: s.Name,
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

func (s *Server)HandleTopics(Topics map[string]TopNodeInfo){

	for topic_name, topic := range Topics {
		_, ok := s.topics[topic_name]
		if !ok {
			top := NewTopic(topic_name)
			top.HandleParttitions(topic.Partitions)

			s.topics[topic_name] = top
		}else{
			DEBUG(dWarn, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}

func (s *Server)CheckList(){
	str, _ := os.Getwd()
	str += "/" + name
	ret := CheckFileOrList(str)
	// DEBUG(dLog, "Check list %v is %v\n", str, ret)
	if !ret {
		CreateList(str)
	}
}

const(
	ErrHadStart = "this partition had Start"
)

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

//准备发送信息，
//检查topic和subscription是否存在，不存在则需要创建
//检查该文件的config是否存在，不存在则创建，并开启协程
//协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in info) (ret string, err error) {

}

// func (s *Server) 

func (s *Server) InfoHandle(ipport string) error {

	DEBUG(dLog, "get consumer's ip_port %v\n", ipport)

	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
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

func (s *Server)StartGet(start startget) (err error) {
	/*
	新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

	查询是否有该订阅的信息；

	PTP：需要负载均衡

	PSB：不需要负载均衡
	*/
	err = nil
	switch start.option{
	case TOPIC_NIL_PTP:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(start.topic_name, start.part_name, start.option)
		ret := s.consumers[start.cli_name].CheckSubscription(sub_name)
		sub := s.consumers[start.cli_name].GetSub(sub_name)
		if ret {  //该订阅存在

			//添加到Config后会进行负载均衡，生成新的配置，然后执行新的配置
			sub.AddConsumerInConfig(start, s.consumers[start.cli_name].GetCli())
			
		}else{    //该订阅不存在
			err = errors.New("this subscription is not exist")
		}

	case TOPIC_KEY_PSB:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(start.topic_name, start.part_name, start.option)
		ret := s.consumers[start.cli_name].CheckSubscription(sub_name)
		
		if ret {  //该订阅存在
			// clis := make(map[string]*client_operations.Client)
			// clis[start.cli_name] = s.consumers[start.cli_name].GetCli()
			// file := s.topics[start.topic_name].GetFile(start.part_name)
			// go s.consumers[start.cli_name].StartPart(start, clis, file)
		}else{    //该订阅不存在
			err = errors.New("this subscription is not exist")
		}
	default:
		err = errors.New("the option is not PTP or PSB")
	}

	return err
}


func (s *Server)CheckConsumer(client *Client){
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList{
			subscription.ShutdownConsumerInGroup(client.name)
		}
		client.mu.Unlock()
	}
}

// subscribe 订阅
func (s *Server) SubHandle(req sub) (err error){
	s.mu.Lock()
	DEBUG(dLog, "get sub information\n")
	top, ok := s.topics[req.topic]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(req)
	if err != nil{
		s.consumers[req.consumer].AddSubScription(sub)
	}
	// resp.parts = GetPartKeyArray(s.topics[req.topic].GetParts())
	// resp.size = len(resp.parts)
	s.mu.Unlock()

	return nil
}

func (s *Server)UnSubHandle(req sub) error {

	s.mu.Lock()
	DEBUG(dLog, "get unsub information\n")
	top, ok := s.topics[req.topic]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub_name, err := top.ReduceSubScription(req)
	if err != nil {
		s.consumers[req.consumer].ReduceSubScription(sub_name)
	}

	s.mu.Unlock()
	return nil
}


func (s *Server) PushHandle(req push) error {
	DEBUG(dLog, "get Message form producer\n")
	topic, ok := s.topics[req.topic]
	// DEBUG(dLog, "after topic\n")
	if !ok {
		DEBUG(dLog, "New a Topic name is %v\n", req.topic)
		topic = NewTopic(req.topic)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}
	topic.addMessage(req)
	
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {

	return retpull{}, nil
}