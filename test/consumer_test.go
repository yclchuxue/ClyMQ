package main

import (
	"fmt"
	"time"

	"testing"
)

const (
	PTP = 1
	PSB = 3
)

func TestConsumer1(t *testing.T) {

	fmt.Println("Test: consumer Subscription")

	zkServer := StartZKServer(t)
	time.Sleep(1*time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1*time.Second)

	consumer := NewConsumerAndStart(t, ":7881", ":7878", "consumer1")
	time.Sleep(1*time.Second)


	fmt.Println("Consumer Sub a Topic")
	err := consumer.Subscription("phone_number", "北京", PTP)
	if err != nil {
		t.Fatal(err.Error())
	}

	consumer.ShutDown_server()
	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Passed")
}
