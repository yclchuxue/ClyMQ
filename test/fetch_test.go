package main

import (
	"ClyMQ/client/clients"
	"fmt"
	"testing"
	"time"
)

func TestFetch(t *testing.T) {
	fmt.Println("Fetch: test producer fetch")

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

	producer := NewProducerAndStart(t, ":7878", "producer2")
	time.Sleep(1 * time.Second)

	//创建topic
	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	//每个topic创建partition
	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "beijing")
	if err != nil {
		t.Fatal(err.Error())
	}

	//将partition设置状态
	err = producer.SetPartitionState("phone_number", "beijing", 1, 3)
	if err != nil {
		t.Fatal(err.Error())
	}

	//等待领导者选出
	time.Sleep(time.Second * 5)

	for _, message := range messages {
		err = producer.Push(clients.Message{
			Topic_name: "phone_number",
			Part_name:  "beijing",
			Msg:        []byte(message),
		}, 1)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	time.Sleep(time.Second * 30)
	//向partitions中生产一些信息

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Passed")
}