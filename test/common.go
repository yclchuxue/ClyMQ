package main

import (
	Server "ClyMQ/server"
	"ClyMQ/zookeeper"
	"fmt"
	"strconv"
	"testing"

	client3 "ClyMQ/client/clients"
)

const ()


func NewConsumerAndStart(t *testing.T, consumer_port, zkbroker, name string) *client3.Consumer {
	fmt.Println("Start Consumser")

	consumer, err := client3.NewConsumer(zkbroker, name, consumer_port)
	if err != nil {
		t.Fatal(err.Error())
	}
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
		broker := Server.NewBrokerAndStart(zookeeper.ZkInfo{
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
	zkserver := Server.NewZKServerAndStart(zookeeper.ZkInfo{
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