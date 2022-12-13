package main

import (
	"ClyMQ/client/clients"
	"fmt"
	"os"

	// "net"
	"time"
)

func main() {
	
	//connection the broker server for push/pull/info
	option := os.Args[1]
	port := ""
	if len(os.Args) == 3{
		port = os.Args[2]
	}else{
		port = "null"
	}
	
	switch option{
	case "p":

		producer, _ := clients.NewProducer("0.0.0.0:2181", "producer1")

		for {
			msg := clients.Message{
				Topic_name:    	"phone_number",
				Part_name:      "yclchuxue",
				Msg:  			[]byte("18788888888"),
			}
			err := producer.Push(msg, -1)
			if err != nil {
				fmt.Println(err)
			}

			time.Sleep(5 * time.Second)
		}

	case "c":	
		consumer,_ := clients.NewConsumer("0.0.0.0:2181", "consumer1", port)
		//start a server for pub and pinpong
		go consumer.Start_server()

		consumer.Subscription("phone_number", "yclchuxue", 2)

		consumer.StartGet(clients.Info{
			Offset: 0,
			Topic: "phone_number",
			Part: "yclchuxue",
			Option: 2,
		})
	}

	//send ip and port for brokerserver can pub this client
}
