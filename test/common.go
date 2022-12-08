package main

import (
	Server "ClyMQ/server"
	"ClyMQ/zookeeper"
	"fmt"
	"net"
	"strconv"
	"testing"

	client3 "ClyMQ/client/clients"

	"github.com/cloudwego/kitex/server"
)

const ()


func NewBrokerAndStart(t *testing.T, zkinfo zookeeper.ZkInfo, opt Server.Options) *Server.RPCServer {
	//start the broker server
	// fmt.Println("Broker_host_Poet", opt.Broker_Host_Port)
	addr_bro, _ := net.ResolveTCPAddr("tcp", opt.Broker_Host_Port)
	addr_raf, _ := net.ResolveTCPAddr("tcp", opt.Raft_Host_Port)
	var opts_bro, opts_raf []server.Option
	opts_bro = append(opts_bro, server.WithServiceAddr(addr_bro))
	opts_raf = append(opts_raf,  server.WithServiceAddr(addr_raf))

	rpcServer := Server.NewRpcServer(zkinfo)

	go func() {
		err := rpcServer.Start(opts_bro, nil, opts_raf, opt)
		if err != nil {
			t.Log(err)
		}
	}()

	return &rpcServer
}

func NewZKServerAndStart(t *testing.T, zkinfo zookeeper.ZkInfo, opt Server.Options) *Server.RPCServer {
	//start the zookeeper server
	addr_zks, _ := net.ResolveTCPAddr("tcp", opt.Zkserver_Host_Port)
	var opts_zks []server.Option
	opts_zks = append(opts_zks, server.WithServiceAddr(addr_zks))

	rpcServer := Server.NewRpcServer(zkinfo)

	go func() {
		err := rpcServer.Start(nil, opts_zks, nil, opt)
		if err != nil {
			t.Log(err)
		}
	}()

	return &rpcServer
}

func NewConsumerAndStart(t *testing.T, consumer_port, zkbroker, name string) *client3.Consumer {
	fmt.Println("Start Consumser")

	consumer, _ := client3.NewConsumer(zkbroker, name, consumer_port)
	go consumer.Start_server()

	return consumer
}

func NewProducerAndStart(t *testing.T, zkbroker, name string) *client3.Producer {
	fmt.Println("Start Producer")
	
	Producer, _ := client3.NewProducer(zkbroker, name)
	
	return Producer
}

func StartBrokers(t *testing.T, numbers int) (brokers []*Server.RPCServer) {
	fmt.Println("Start Brokers")
	
	zookeeper_port := []string{"127.0.0.1:2181"}
	server_ports := []string{":7774", ":7775", ":7776"}
	// consumer_ports := []string{":7881", ":7882", ":7883"}
	raft_ports := []string{":7331", ":7332", ":7333"}

	index := 0
	for index < numbers {
		broker := NewBrokerAndStart(t, zookeeper.ZkInfo{
			HostPorts: zookeeper_port,
			Timeout:   20,
			Root:      "/ClyMQ",
		}, Server.Options{
			Me:  			  index,	
			Name:             "Broker" + strconv.Itoa(index),
			Tag:              "broker",
			Broker_Host_Port: server_ports[index],
			Raft_Host_Port:   raft_ports[index],
			Zkserver_Host_Port: ":7878",
		})
		index++
		brokers = append(brokers, broker)
	}

	return brokers
}

func StartZKServer(t *testing.T) *Server.RPCServer {
	
	fmt.Println("Start ZKServer")
	
	zookeeper_port := []string{"127.0.0.1:2181"}
	zkserver := NewZKServerAndStart(t, zookeeper.ZkInfo{
		HostPorts: zookeeper_port,
		Timeout:   20,
		Root:      "/ClyMQ",
	}, Server.Options{
		Name: "ZKServer",
		Tag:  "zkbroker",
		Zkserver_Host_Port: ":7878",
	})

	return zkserver
}

func ShutDownBrokers(brokers []*Server.RPCServer) {
	for _, ser := range brokers {
		ser.ShutDown_server()
	}
}

func ShutDownZKServer(zkserver *Server.RPCServer) {
	zkserver.ShutDown_server()
}