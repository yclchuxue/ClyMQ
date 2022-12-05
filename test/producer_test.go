package main

import (
	"fmt"

	"testing"
	"time"

)

func TestProducerCreate(t *testing.T) {

	fmt.Println("Test: producer Create Topic and Partition")

	zkServer := StartZKServer(t)
	time.Sleep(1*time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1*time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1*time.Second)


	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "xian")
	if err != nil {
		t.Fatal(err.Error())
	}

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Passed")
}

func TestProducerPush(t *testing.T) {
	fmt.Println("Test: producer Push message to Partition")

	zkServer := StartZKServer(t)
	time.Sleep(1*time.Second)

	brokers := StartBrokers(t, 1)
	time.Sleep(1*time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1*time.Second)


	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "xian")
	if err != nil {
		t.Fatal(err.Error())
	}

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("  ... Passed")
}
