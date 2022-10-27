package main

import (
	"ClyMQ/kitex_gen/api"
	"context"
	"fmt"
)

// OperationsImpl implements the last service interface defined in the IDL.
type OperationsImpl struct{}

// Push implements the OperationsImpl interface.
func (s *OperationsImpl) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {
	// TODO: Your code here...
	fmt.Println(req)
	return &api.PushResponse{
		Ret: true,
	}, nil
}

// Pull implements the OperationsImpl interface.
func (s *OperationsImpl) Pull(ctx context.Context, req *api.PullRequest) (resp *api.PullResponse, err error) {
	// TODO: Your code here...
	return &api.PullResponse{Message: "18788888888"}, nil
}
