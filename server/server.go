package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	"errors"
	"sync"

	client2 "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]*Topic
	// groups map[string]Group

	consumers map[string]*Client

	// sublist map[string]*SubScription

	mu sync.Mutex
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

type startget struct{
	cli_name  	string
    topic_name	string
    part_name	string
    offset		int64
}

func (s *Server) make() {

	s.topics = make(map[string]*Topic)
	// s.groups = make(map[string]Group)
	// s.sublist = make(map[string]*SubScription)
	s.consumers = make(map[string]*Client)
	// s.groups["default"] = Group{}

	s.mu = sync.Mutex{}

	s.StartRelease()
}

func (s* Server)StartRelease(){
	s.mu.Lock()
	for _, topic := range s.topics {
		go topic.StartRelease(s)
	}
	s.mu.Unlock()
}

func (s *Server) InfoHandle(ipport string) error {

	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
	if err == nil {
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		go s.RecoverConsumer(consumer)
		s.mu.Unlock()

		return nil
	}

	return err
}

func (s *Server)StartGet(start startget) error {
	
}

func (s *Server)RecoverConsumer(client *Client){
	s.mu.Lock()
	client.mu.Lock()
	client.state = ALIVE
	for sub_name, sub := range client.subList{
		go s.topics[sub.topic_name].RecoverRelease(sub_name, client.name)
	}
	client.mu.Unlock()	
	s.mu.Unlock()
}

func (s *Server)CheckConsumer(client *Client){
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList{
			topic := subscription.ShutdownConsumer(client.name)
			s.topics[topic].Rebalance()
		}
		client.mu.Unlock()
	}
}


// subscribe 订阅
func (s *Server) SubHandle(req sub) error{
	s.mu.Lock()
	top, ok := s.topics[req.topic]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(req, s.consumers[req.consumer])
	if err != nil{
		s.consumers[req.consumer].AddSubScription(sub)
	}

	s.mu.Unlock()

	return nil
}

func (s *Server)UnSubHandle(req sub) error {

	s.mu.Lock()
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

func (s *Server) addMessage(topic *Topic, req push) error {
	part, ok := topic.Parts[req.key]
	if !ok {
		part = NewPartition(req)

		go part.Release(s)  //创建新分片后，开启协程发送消息

		topic.Parts[req.key] = part
	}else{
		part.rmu.Lock()
		part.queue = append(part.queue, req.message)
		part.rmu.Unlock()
	}

	return nil
}

func (s *Server) PushHandle(req push) error {

	topic, ok := s.topics[req.topic]
	if !ok {
		topic = NewTopic(req)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}else{
		s.addMessage(topic, req)
	}
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {



	return retpull{}, nil
}