package main

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/operations"
	"context"
	"fmt"
	client2 "github.com/cloudwego/kitex/client"
)

func main() {
	client, err := operations.NewClient("client", client2.WithHostPorts("0.0.0.0:8888"))
	if err != nil {
		fmt.Println(err)
	}

	req := &api.PushRequest{
		Producer: int64(1),
		Topic:    "phone number",
		Key:      "yclchuxue",
		Message:  "18788888888",
	}
	resp, err := client.Push(context.Background(), req)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)
}
