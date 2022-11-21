package server

import (
	"ClyMQ/raft"
)

type parts_raft struct {
	Partitions 	map[string]*raft.Raft
}

func NewParts_Raft() *parts_raft {
	return &parts_raft{
		Partitions: make(map[string]*raft.Raft),
	}
}

//添加一个需要raft同步的partition
func (p *parts_raft)AddPart_Raft() {

}

//设置partitoin中谁来做leader
func (p *parts_raft)SetLeader() {

}