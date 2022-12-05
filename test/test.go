package main

import (
	"fmt"
	"testing"
	"time"
)

//测试前我们需要对zookeeper和各个集群进行初始化一些数据方便后续的测试
func init() {
	fmt.Println("Init: init status for test")

    var  t *testing.T

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1 * time.Second)

    //创建三个topic
	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

    //每个topic创建三个partition
	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "xian")
	if err != nil {
		t.Fatal(err.Error())
	}

    //向partitions中生产一些信息

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Init Successful")
}
