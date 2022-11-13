package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	"errors"
	"os"
	"sync"

	client2 "github.com/cloudwego/kitex/client"
)

var (
	name string
)

const (
	NODE_SIZE  =  42
)

type Server struct {
	topics map[string]*Topic
	// groups map[string]Group

	consumers map[string]*Client

	// sublist map[string]*SubScription

	mu sync.RWMutex
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

type retsub struct{
	size int
	parts []PartKey
}

type startget struct{
	cli_name  	string
    topic_name	string
    part_name	string
    index		int64
	option 		int8
}

func NewServer() *Server {
	return &Server{
		// topics: make(map[string]*Topic),
		// consumers: make(map[string]*Client),
		mu:sync.RWMutex{},
	}
}

func (s *Server) make() {

	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)

	name = GetIpport()
	
	s.CheckList()
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
func (s *Server) SubHandle(req sub) (resp retsub, err error){
	s.mu.Lock()
	DEBUG(dLog, "get sub information\n")
	top, ok := s.topics[req.topic]
	if !ok {
		return resp, errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(req)
	if err != nil{
		s.consumers[req.consumer].AddSubScription(sub)
	}
	resp.parts = GetPartKeyArray(s.topics[req.topic].GetParts())
	resp.size = len(resp.parts)
	s.mu.Unlock()

	return resp, nil
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
		topic = NewTopic(req)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}else{
		topic.addMessage(req)
	}
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {

	return retpull{}, nil
}