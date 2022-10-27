package server

import (
	"ClyMQ/kitex_gen/api"
	"context"
	"fmt"
)

type server struct {
	me int64
}

func (s *server) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {
	fmt.Println(req)
	return &api.PushResponse{
		Ret: true,
	}, nil
}

func (s *server) Pull(ctx context.Context, req api.PullRequest) (resp *api.PullResponse, err error) {
	return &api.PullResponse{Message: "111"}, nil
}
