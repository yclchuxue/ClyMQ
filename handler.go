package main

import (
	"ClyMQ/kitex_gen/api"
	"context"
)

type Client_OperationsImpl struct{}

// Pub implements the Client_OperationsImpl interface.
func (s *Client_OperationsImpl) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	// TODO: Your code here...
	return &api.PubResponse{Ret: true}, nil
}

// Pingpong implements the Client_OperationsImpl interface.
func (s *Client_OperationsImpl) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	// TODO: Your code here...
	return &api.PingPongResponse{Pong: true}, err
}