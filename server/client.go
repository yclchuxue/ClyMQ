package server

import "ClyMQ/kitex_gen/api/client_operations"


// type Client struct{
// 	consumer client_operations.Client
// }

type Group struct{
	topics []*Topic
	consumers map[string]*client_operations.Client
}