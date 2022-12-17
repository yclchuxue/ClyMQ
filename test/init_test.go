package main

import (
	"ClyMQ/client/clients"
	"ClyMQ/server"
	"fmt"
	"testing"
	"time"
)

//测试前我们需要对zookeeper和各个集群进行初始化一些数据方便后续的测试
func TestInit1(t *testing.T) {
	fmt.Println("Init: init status for test")

	messages := []string{"18700619719", "1234567891",
		"12345678911", "12345678912",
		"12345678913", "12345678914",
		"12345678915", "12345678916",
		"12345678917", "12345678918",
		"12345678919"}

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1 * time.Second)

	//创建topic
	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	//每个topic创建partition
	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "xian")
	if err != nil {
		t.Fatal(err.Error())
	}

	//将partition设置状态
	err = producer.SetPartitionState("phone_number", "xian", -1, 3)
	if err != nil {
		t.Fatal(err.Error())
	}

	//等待领导者选出
	time.Sleep(time.Second * 5)

	for _, message := range messages {
		err = producer.Push(clients.Message{
			Topic_name: "phone_number",
			Part_name:  "xian",
			Msg:        []byte(message),
		}, -1)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	time.Sleep(time.Second * 30)
	//向partitions中生产一些信息

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Init Successful")
}

func TestInit2(t *testing.T) {
	fmt.Println("Init: init status for test")

	// messages := []string{"18700619719", "1234567891",
	// 	"12345678911", "12345678912",
	// 	"12345678913", "12345678914",
	// 	"12345678915", "12345678916",
	// 	"12345678917", "12345678918",
	// 	"12345678919"}

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	consumer := NewConsumerAndStart(t, ":7881", ":7878", "consumer1")

	time.Sleep(1 * time.Second)

	parts, ret, err := consumer.StartGet(clients.Info{
		Topic: "phone_number",
		Part: "xian",
		Option: server.TOPIC_NIL_PTP_PULL,
	})
	if err != nil {
		t.Fatal(ret, err.Error())
	}

	for _, part := range parts {
		cli, err := consumer.GetCli(part)
		if err != nil {
			t.Fatal(err.Error())
		}
		info := clients.NewInfo(0, "phone_number", "xian")
		info.Cli = cli
		info.Option = server.TOPIC_NIL_PTP_PULL
		info.Size = 10
		start, end, msgs, err := consumer.Pull(info)
		if err != nil {
			t.Fatal(err.Error())
		}
		
		fmt.Println("start ", start, "end ", end)
		for _, msg := range msgs {
			fmt.Println(msg.Topic_name, msg.Part_name, msg.Index, string(msg.Msg))
		}
	} 

	time.Sleep(time.Second * 30)
	//向partitions中生产一些信息

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Init Successful")
}