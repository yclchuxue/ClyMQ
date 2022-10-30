package server

import (
	"sync"
	"time"
)

const (
	TOPIC_NIL_PTP = 1 //
	TOPIC_NIL_PSB = 2
	TOPIC_KEY_PSB = 3 //map[cli_name]offset in a partition
)

type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition
	subList map[string]*SubScription
}
type Partition struct {
	rmu              sync.RWMutex
	key              string
	queue            []string
	consumers_offset map[string]int
}
type SubScription struct {
	rmu              sync.RWMutex
	topic_name       string
	option           int8
	consumer_to_part map[string]string //consumer to partition
	groups           []*Group
}

func (s *SubScription) ShutdownConsumer(cli_name string) {
	s.rmu.Lock()

	switch s.option{
	case TOPIC_NIL_PTP:// point to point just one group
		s.groups[0].DownClient(cli_name)
	case TOPIC_KEY_PSB:
		for _, group := range s.groups {
			group.DownClient(cli_name)
		}
	}

	s.rmu.Unlock()

	s.Rebalance()
}

func (s *SubScription) AddConsumer(req sub) {

	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].AddClient(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)
		s.consumer_to_part[req.consumer] = req.key
	}
}

func (s *SubScription) Rebalance(){

}

func NewTopic(req push) *Topic {
	topic := &Topic{
		rmu: sync.RWMutex{},
		Parts: make(map[string]*Partition),
		subList: make(map[string]*SubScription),
	}
	part := NewPartition(req)
	topic.Parts[req.key] = part

	return topic
}

// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func (t *Topic) getStringfromSub(req sub) string {
	ret := req.topic
	if req.option == TOPIC_NIL_PTP { // 订阅发布模式
		ret = ret + "NIL" + "ptp" //point to point
	} else if req.option == TOPIC_KEY_PSB {
		ret = ret + req.key + "psb" //pub and sub
	} else if req.option == TOPIC_NIL_PSB {
		ret = ret + "NIL" + "psb"
	}
	return ret
}

func (t *Topic) AddSubScription(req sub) (retsub *SubScription, err error){
	ret := t.getStringfromSub(req)

	subscription, ok := t.subList[ret]
	if !ok {
		subscription = NewSubScription(req)
	} else {
		subscription.AddConsumer(req)
	}

	return subscription, nil
}

func NewSubScription(req sub) *SubScription {
	sub := &SubScription{
		rmu: sync.RWMutex{},
		topic_name:       req.topic,
		option:           req.option,
		consumer_to_part: make(map[string]string),
	}

	group := NewGroup(req.topic, req.consumer)
	sub.groups = append(sub.groups, group)
	sub.consumer_to_part[req.consumer] = req.key

	return sub
}

func NewPartition(req push)*Partition{
	part := &Partition{
		rmu: sync.RWMutex{},
		key: req.key,
		queue: make([]string, 40),
		consumers_offset: make(map[string]int),
	}

	part.queue = append(part.queue, req.message)

	return part
}

func (p *Partition) Release(server *Server) {
	for consumer_name := range p.consumers_offset {
		server.mu.Lock()
		con := server.consumers[consumer_name]
		server.mu.Unlock()

		go p.Pub(con)
	}
}

func (p *Partition)Pub(cli *Client){

	for{
		cli.mu.RLock()
		if cli.state == ALIVE {

			name := cli.name
			cli.mu.RUnlock()
			
			p.rmu.RLock()
			
			offset := p.consumers_offset[name]
			msg := p.queue[offset]
			p.rmu.RUnlock()

			ret := cli.Pub(msg)
			if ret {
				p.rmu.Lock()
				p.consumers_offset[name] = offset+1
				p.rmu.Unlock()
			}
			
		}else{
			cli.mu.RUnlock()
			time.Sleep(time.Second)
		}
	}	
}


func (t *Topic)StartRelease(server *Server){
	for _, part := range t.Parts{
		part.Release(server)   //创建心分片后，开启协程发送消息
	}
}