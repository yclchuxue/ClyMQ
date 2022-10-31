package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/client_operations"
	"context"
	"errors"
	"sync"
	"time"
)

const (
	ALIVE = "alive"
	DOWN = "down"
)

type Client struct{
	mu sync.RWMutex
	name string
	consumer client_operations.Client
	subList map[string]*SubScription  // 若这个consumer关闭则遍历这些订阅并修改
	// ingroups []*Group
	state string
}

type Group struct{
	rmu sync.RWMutex
	topic_name string
	consumers map[string]bool // map[client'name]alive
}

func NewClient(ipport string, con client_operations.Client) *Client{
	client := &Client{
		mu: sync.RWMutex{},
		name: ipport,
		consumer: con,
		state: ALIVE,
		subList: make(map[string]*SubScription),
	}
	return client
}

func NewGroup(topic_name, cli_name string)*Group{
	group := &Group{
		rmu: sync.RWMutex{},
		topic_name: topic_name,
	}
	group.consumers[cli_name] = true
	return group
}

func (g *Group)RecoverClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()

	_, ok := g.consumers[cli_name]
	if ok {
		if g.consumers[cli_name] {
			return errors.New("This client is alive before")
		}else{
			g.consumers[cli_name] = true
			return nil
		}
		return nil
	}else{
		return errors.New("Do not have this client")
	}	
}

func (g *Group)AddClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	_, ok := g.consumers[cli_name]
	if ok {
		return errors.New("this client has in this group")
	}else{
		g.consumers[cli_name] = true
		return nil
	}
}

func (g *Group)DownClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

func (g *Group)DeleteClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}

func (c *Client)CheckConsumer() bool { //心跳检测
	c.mu = sync.RWMutex{}

	for{
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{Ping: true})
		if err != nil || !resp.Pong {
			break
		}

		time.Sleep(time.Second)
	}
	c.mu.Lock()
	c.state = DOWN
	c.mu.Unlock()
	return true
}

func (c *Client)AddSubScription(sub *SubScription){
	c.mu.Lock()
	c.subList[sub.name] = sub
	c.mu.Unlock()
}

func (c *Client)ReduceSubScription(name string){
	c.mu.Lock()
	delete(c.subList, name)
	c.mu.Unlock()
}

// publish 发布
func (c *Client)Pub(message string) bool {

	resp, err := c.consumer.Pub(context.Background(), &api.PubRequest{Meg: message})

	if err != nil || !resp.Ret {
		return false
	}

	return true
}