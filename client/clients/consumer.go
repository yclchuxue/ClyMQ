package clients

import (
	"ClyMQ/kitex_gen/api"
	ser "ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"ClyMQ/logger"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

type Consumer struct {
	mu    sync.RWMutex
	Name  string
	State string

	srv      server.Server
	port     string
	zkBroker zkserver_operations.Client
	Brokers  map[string]*server_operations.Client //broker_name--client
	// PTP_Topics 	map[string]
	// Topic_Partions map[string]Info
}

func NewConsumer(zkbroker string, name string, port string) (*Consumer, error) {
	C := Consumer{
		mu:      sync.RWMutex{},
		Name:    name,
		State:   "alive",
		port:    port,
		Brokers: make(map[string]*server_operations.Client),
		// Topic_Partions: make(map[string]Info),
	}
	var err error
	fmt.Println("the zkbroker address is ", zkbroker, "the port is ", port)
	C.zkBroker, err = zkserver_operations.NewClient(C.Name, client.WithHostPorts(zkbroker))

	return &C, err
}

func (c *Consumer) Alive() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.State
}

func (c *Consumer) Start_server() {
	// fmt.Println("port is ", c.port)
	addr, _ := net.ResolveTCPAddr("tcp", c.port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(c, opts...)
	c.srv = svr
	err := svr.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (c *Consumer) ShutDown_server() {
	c.srv.Stop()
}

func (c *Consumer) Down() {
	c.mu.Lock()
	c.State = "down"
	c.mu.Unlock()
}

func (c *Consumer) SendInfo(port string, cli *server_operations.Client) error {

	resp, err := (*cli).ConInfo(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Println(resp)
	}
	return err
}

func (c *Consumer) Subscription(topic, partition string, option int8) (err error) {
	//向zkserver订阅topic或partition，
	c.mu.RLock()
	zk := c.zkBroker
	c.mu.RUnlock()

	resp, err := zk.Sub(context.Background(), &api.SubRequest{
		Consumer: c.Name,
		Topic:    topic,
		Key:      partition,
		Option:   option,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (c *Consumer) StartGet(info Info) (partkeys []PartKey, ret string, err error) {

	resp, err := c.zkBroker.ConStartGetBroker(context.Background(), &api.ConStartGetBrokRequest{
		CliName:   c.Name,
		TopicName: info.Topic,
		PartName:  info.Part,
		Option:    info.Option,
		Index:     info.Offset,
	})

	if err != nil || !resp.Ret {
		return nil, ret, err
	}

	// broks := make([]BrokerInfo, resp.Size)
	// json.Unmarshal(resp.Broks, &broks)

	// parts = make([]PartKey, resp.Size)
	var parts Parts
	err = json.Unmarshal(resp.Parts, &parts)
	if err != nil {
		return nil, "", err
	}
	logger.DEBUG(logger.DLog, "start get parts json is %v turn %v\n", resp.Parts, parts.PartKeys)
	if info.Option == 1 || info.Option == 3 { //pub
		ret, err = c.StartGetToBroker(parts.PartKeys, info)
	}
	return parts.PartKeys, ret, err
}

func (c *Consumer) StartGetToBroker(parts []PartKey, info Info) (ret string, err error) {

	//连接上各个broker，并发送start请求

	for _, part := range parts {

		if part.Err != "ok" {
			ret += part.Err
			continue
		}

		rep := &api.InfoGetRequest{
			CliName:   c.Name,
			TopicName: info.Topic,
			PartName:  part.Name,
			Option:    info.Option,
			Offset:    info.Offset,
		}

		bro_cli, ok := c.Brokers[part.Broker_name]
		if !ok {
			bro_cli, err := server_operations.NewClient(c.Name, client.WithHostPorts(part.Broker_H_P))
			if err != nil {
				return ret, err
			}
			if info.Option == 1 { //ptp
				c.Brokers[part.Broker_name] = &bro_cli

				bro_cli.StarttoGet(context.Background(), rep)
			}
		}
		//发送info
		err = c.SendInfo(c.port, bro_cli)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return ret, err
		}

		if info.Option == 3 { //psb
			(*bro_cli).StarttoGet(context.Background(), rep)
		}
	}
	return ret, nil
}

func (c *Consumer) GetCli(part PartKey) (cli *server_operations.Client, err error) {
	cli, ok := c.Brokers[part.Broker_name]
	if !ok {
		bro_cli, err := server_operations.NewClient(c.Name, client.WithHostPorts(part.Broker_H_P))
		if err != nil {
			return nil, err
		}
		cli = &bro_cli
		c.Brokers[part.Broker_name] = cli
		logger.DEBUG(logger.DLog, "get cli(%v) fail, new client\n", part.Broker_name)
	}

	return cli, nil
}

//向broker索要信息
func (c *Consumer) Pull(info Info) (int64, int64, []Msg, error) {
	resp, err := (*info.Cli).Pull(context.Background(), &api.PullRequest{
		Consumer: c.Name,
		Topic:    info.Topic,
		Key:      info.Part,
		Offset:   info.Offset,
		Size:     info.Size,
		Option:   info.Option,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return -1, -1, nil, err
	}

	msgs := make([]Msg, resp.EndIndex-resp.StartIndex)
	err = json.Unmarshal(resp.Msgs, &msgs)
	if err != nil {
		return 0, 0, nil, err
	}
	if resp.Err == "file EOF" {
		return 0, 0, nil, io.EOF
	}

	return resp.StartIndex, resp.EndIndex, msgs, nil
}

type Msg struct {
	Index      int64  `json:"index"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

type Info struct {
	Offset int64
	Topic  string
	Part   string
	Option int8
	Size   int8
	Cli    *server_operations.Client
	Bufs   map[int64]*api.PubRequest
}

func NewInfo(offset int64, topic, part string) Info {
	return Info{
		Offset: offset,
		Topic:  topic,
		Part:   part,
		Bufs:   make(map[int64]*api.PubRequest),
	}
}

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req)

	/*
		添加用户自己的处理代码
	*/

	return &api.PubResponse{Ret: true}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	fmt.Println("PingPong")

	return &api.PingPongResponse{Pong: true}, nil
}
