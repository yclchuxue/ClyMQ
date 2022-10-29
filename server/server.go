package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	client2 "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]Topic
	groups map[string]Group
}

func (s *Server)make(){

	s.topics = make(map[string]Topic)
	s.groups = make(map[string]Group)

	s.groups["default"] = Group{}

}

func (s *Server)InfoHandle(ipport string) error {

	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
	if err == nil {
		s.groups["default"].consumers[ipport] = &client

		return nil
	}

	return err
}

func (s *Server)PushHandle()

func (s *Server)PullHandle()