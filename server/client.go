package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/client_operations"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

const (
	ALIVE = "alive"
	DOWN  = "down"
)

type Client struct {
	mu       sync.RWMutex
	name     string
	state    string
	consumer client_operations.Client
	parts    map[string]Part

	// ingroups []*Group
	subList map[string]*SubScription // 若这个consumer关闭则遍历这些订阅并修改
}

func NewClient(ipport string, con client_operations.Client) *Client {
	client := &Client{
		mu:       sync.RWMutex{},
		name:     ipport,
		consumer: con,
		state:    ALIVE,
		parts:    make(map[string]Part),
		subList:  make(map[string]*SubScription),
	}
	return client
}

func (c *Client) CheckConsumer() bool { //心跳检测
	c.mu = sync.RWMutex{}

	for {
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

func (c *Client) CheckSubscription(sub_name string) bool {
	c.mu.RLock()
	_, ok := c.subList[sub_name]
	c.mu.Unlock()

	return ok
}

func (c *Client) AddSubScription(sub *SubScription) {
	c.mu.Lock()
	c.subList[sub.name] = sub
	c.mu.Unlock()
}

func (c *Client) ReduceSubScription(name string) {
	c.mu.Lock()
	delete(c.subList, name)
	c.mu.Unlock()
}

func (c *Client) GetStat() string{
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.state
}

func (c *Client) GetCli() *client_operations.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &c.consumer
}

func (c *Client) StartPart(start startget, clis map[string]*client_operations.Client, file *File) {
	c.mu.Lock()
	part, ok := c.parts[start.part_name]
	if !ok {
		part = NewPart(start, clis, file)
		c.parts[start.part_name] = part
	}
	go part.Start()
	c.mu.Unlock()
}

type Part struct {
	mu         sync.RWMutex
	topic_name string
	part_name  string
	option     int8
	clis       map[string]*client_operations.Client

	state string
	fd    os.File
	file  *File

	index  int64 //use index to find offset
	offset int64

	start_index int64
	end_index   int64

	buffer_node map[int64]Key
	buffer_msg  map[int64][]Message

	part_had chan Done
	buf_done map[int64]string
}

const (
	OK      = "ok"
	TIMEOUT = "timeout"

	NOTDO  = "notdo"
	HAVEDO = "havedo"
	HADDO  = "haddo"

	BUFF_NUM  = 5
	AGAIN_NUM = 3
)

type Done struct {
	in  int64
	err string
	name string
	cli *client_operations.Client
	// add a consumer name for start to send
}

func NewPart(start startget, clis map[string]*client_operations.Client, file *File) Part {
	return Part{
		mu:          sync.RWMutex{},
		topic_name:  start.topic_name,
		part_name:   start.part_name,
		option:      start.option,
		index:       start.index,
		buffer_node: make(map[int64]Key),
		buffer_msg:  make(map[int64][]Message),
		file:        file,
		clis:        clis,
		state:       ALIVE,

		part_had: make(chan Done),
		buf_done: make(map[int64]string),
	}
}

func (p *Part) Start() {

	
	// open file
	p.fd = *p.file.OpenFile()
	offset, err := p.file.FindOffset(&p.fd, p.index)

	if err != nil {
		DEBUG(dError, err.Error())
	}

	p.offset = offset

	for i := 0; i < BUFF_NUM; i++ { //加载 BUFF_NUM个block到队列中
		err := p.AddBlock()
		if err != nil {
			DEBUG(dError, err.Error())
		}
	}

	go p.GetDone()

	for {
		p.mu.RLock()
		if p.state == DOWN {
			break
		}
		p.mu.RUnlock()

		/*
			循环clis，按块发送信息，例如两个consumer消费这个part，我们go两个协程去发送，
			go后使用条件变量或select管道来判断是否成功，成功则继续，失败或超时则另外考虑；

			从文件取出后将块序号放入 buf_do
			从管道读出确认后将序号放入 buf_done
		*/

		p.mu.RLock()
		for name, cli := range p.clis {
			go p.SendOneBlock(name, cli, p.start_index)
		}
		p.mu.RUnlock()
	}
}

func (p *Part) AddConsumer() {

}

func (p *Part) AddBlock() error {

	node, msg, err := p.file.ReadFile(&p.fd, p.offset)

	if err != nil {
		return err
	}

	p.mu.Lock()
	p.buf_done[node.Start_index] = NOTDO //新加入的块未被消费
	p.buffer_node[node.Start_index] = node
	p.buffer_msg[node.Start_index] = msg

	p.end_index = node.End_index
	p.offset += int64(NODE_SIZE) + int64(node.Size)

	p.mu.Unlock()

	return err
}

func (p *Part) GetDone() {

	for {
		select {
		case do := <-p.part_had:
			if do.err == OK { // 发送成功，buf_do--, buf_done++, 补充buf_do

				err := p.AddBlock()

				p.mu.Lock()
				if err != nil {
					DEBUG(dError, err.Error())
				}

				p.buf_done[do.in] = HADDO
				in := p.start_index

				for {
					if p.buf_done[in] == HADDO {
						p.start_index = p.buffer_node[in].End_index + 1
						delete(p.buf_done, in)
						delete(p.buffer_msg, in)
						delete(p.buffer_node, in)
						in = p.start_index
					} else {
						break
					}
				}

				go p.SendOneBlock(do.name, do.cli, p.start_index)

				p.mu.Unlock()

			}
			if do.err == TIMEOUT { 	//超时  已尝试发送3次
									//认为该消费者掉线
				p.mu.Lock()
				delete(p.clis, do.name)    	//删除该消费者    考虑是否需要    
											//判断是否有消费者存在，若无则关闭协程和文件描述符
				p.mu.Unlock()
			}
		}
	}
}

func (p *Part) SendOneBlock(name string, cli *client_operations.Client, start_index int64) {

	in := start_index
	num := 0
	for {
		p.mu.Lock()
		if p.buf_done[in] == NOTDO {

			msg, ok1 := p.buffer_msg[in]
			node, ok2 := p.buffer_node[in]

			if !ok1 || !ok2 {
				DEBUG(dError, "get msg and node from buffer the in = %v\n", in)
			}
			p.buf_done[in] = HAVEDO
			p.mu.Unlock()

			data_msg, _ := json.Marshal(msg)

			for {
				err := p.Pub(cli, node, data_msg)

				if err != nil { //超时等原因
					DEBUG(dError, err.Error())
					num++
					if num >= AGAIN_NUM {  //超时三次，将不再向其发送
						p.part_had <- Done{
							in:  node.Start_index,
							err: TIMEOUT,
							name: name,
							cli: cli,
						}

						p.mu.Lock()
						p.buf_done[in] = NOTDO
						p.mu.Unlock()

						break
					}

				} else {
					p.part_had <- Done{
						in:  node.Start_index,
						err: OK,
						name: name,
						cli: cli,
					}
					break
				}
			}
			p.mu.Unlock()
			break
		} else {
			in = p.buffer_node[in].End_index + 1
		}
		p.mu.Unlock()
	}

}

// publish 发布
func (p *Part) Pub(cli *client_operations.Client, node Key, data []byte) error {
	resp, err := (*cli).Pub(context.Background(),
		&api.PubRequest{
			TopicName:  p.topic_name,
			PartName:   p.part_name,
			StartIndex: node.Start_index,
			EndIndex:   node.End_index,
			Msg:        data,
		})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

func (p *Part) ClosePart() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state = DOWN
}

type Group struct {
	rmu        sync.RWMutex
	topic_name string
	consumers  map[string]bool // map[client'name]alive
}

func NewGroup(topic_name, cli_name string) *Group {
	group := &Group{
		rmu:        sync.RWMutex{},
		topic_name: topic_name,
	}
	group.consumers[cli_name] = true
	return group
}

func (g *Group) RecoverClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()

	_, ok := g.consumers[cli_name]
	if ok {
		if g.consumers[cli_name] {
			return errors.New("This client is alive before")
		} else {
			g.consumers[cli_name] = true
			return nil
		}
	} else {
		return errors.New("Do not have this client")
	}
}

func (g *Group) AddClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	_, ok := g.consumers[cli_name]
	if ok {
		return errors.New("this client has in this group")
	} else {
		g.consumers[cli_name] = true
		return nil
	}
}

func (g *Group) DownClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

func (g *Group) DeleteClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}
