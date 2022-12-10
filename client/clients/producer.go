package clients

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"context"
	"errors"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type Producer struct {
	mu sync.RWMutex

	Name            string
	ZkBroker        zkserver_operations.Client
	Topic_Partions  map[string]server_operations.Client //map[topicname+partname]cli 表示该Topic的分片是否是这个producer负责
	Top_Part_indexs map[string]int64
}

type Message struct {
	Topic_name string
	Part_name  string
	Msg        []byte
}

func NewProducer(zkbroker string, name string) (*Producer, error) {
	P := Producer{
		mu:             sync.RWMutex{},
		Name:           name,
		Topic_Partions: make(map[string]server_operations.Client),
		Top_Part_indexs: make(map[string]int64),
	}
	var err error
	P.ZkBroker, err = zkserver_operations.NewClient(P.Name, client.WithHostPorts(zkbroker))

	return &P, err
}

func (p *Producer) CreateTopic(topic_name string) error {

	resp, err := p.ZkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topic_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

func (p *Producer) CreatePart(topic_name, part_name string) error {
	resp, err := p.ZkBroker.CreatePart(context.Background(), &api.CreatePartRequest{
		TopicName: topic_name,
		PartName:  part_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

func (p *Producer) SetPartitionState(topic_name, part_name string, option, dupnum int8) error {
	resp, err := p.ZkBroker.SetPartitionState(context.Background(), &api.SetPartitionStateRequest{
		Topic:     topic_name,
		Partition: part_name,
		Option:    option,
		Dupnum:    dupnum,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) Push(msg Message, ack int) error {
	str := msg.Topic_name + msg.Part_name
	var ok2 bool
	var index int64
	p.mu.RLock()
	cli, ok1 := p.Topic_Partions[str]
	if ack == -1 { //raft, 获取index
		index, ok2 = p.Top_Part_indexs[str]
	}
	zk := p.ZkBroker
	p.mu.RUnlock()

	if !ok1 {
		resp, err := zk.ProGetBroker(context.Background(), &api.ProGetBrokRequest{
			TopicName: msg.Topic_name,
			PartName:  msg.Part_name,
		})

		if err != nil || !resp.Ret {
			return err
		}

		cli, err = server_operations.NewClient(p.Name, client.WithHostPorts(resp.BrokerHostPort))

		if err != nil {
			return err
		}

		p.mu.Lock()
		p.Topic_Partions[str] = cli
		p.mu.Unlock()
	}

	
	if ack == -1 {
		if !ok2 {
			p.mu.Lock()
			p.Top_Part_indexs[str] = 0
			p.mu.Unlock()
		}
	}
	

	//若partition所在的broker发生改变，将返回信息，重新请求zkserver
	resp, err := cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic_name,
		Key:      msg.Part_name,
		Message:  msg.Msg,
		Ack:      int8(ack),
		Cmdindex: index,
	})
	if err == nil && resp.Ret {
		p.mu.Lock()
		p.Top_Part_indexs[str] = index+1
		p.mu.Unlock()

		return nil
	} else if resp.Err == "partition remove" {
		p.mu.Lock()
		delete(p.Topic_Partions, str)
		p.mu.Unlock()

		return p.Push(msg, ack) //重新发送该信息

	} else {
		return errors.New("err != " + err.Error() + "or resp.Ret == false")
	}
}
