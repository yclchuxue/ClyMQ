package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/raft_operations"
	"ClyMQ/raft"
	"context"
	"encoding/json"
	"net"
	"sync"

	"github.com/cloudwego/kitex/server"
)

type parts_raft struct {
	mu sync.RWMutex
	srv_raft 	server.Server
	Partitions 	map[string]*raft.Raft
}

func NewParts_Raft() *parts_raft {
	return &parts_raft{
		mu: sync.RWMutex{},
		Partitions: make(map[string]*raft.Raft),
	}
}

func (p *parts_raft)make(name, host_port string){
	addr,_ := net.ResolveIPAddr("tcp", host_port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	p.srv_raft = raft_operations.NewServer(p, opts...)

	err := p.srv_raft.Run()
	if err != nil {
		DEBUG(dError, err.Error())
	}
}

//添加一个需要raft同步的partition
func (p *parts_raft)AddPart_Raft(peers []*raft_operations.Client, me int, topic_name, part_name string, applyCh chan raft.ApplyMsg) {

	//启动一个raft，即调用Make(), 需要提供各节点broker的 raft_clients, 和该partition的管道，
	per := &raft.Persister{}
	part_raft := raft.Make(peers, me,per, applyCh, topic_name, part_name)
	
	p.mu.Lock()
	p.Partitions[topic_name+part_name] = part_raft
	p.mu.Unlock()
}

//设置partitoin中谁来做leader
func (p *parts_raft)SetLeader() {

}

func (p *parts_raft)RequestVote(ctx context.Context, rep *api.RequestVoteArgs_) (r *api.RequestVoteReply, err error){
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	resp :=  p.Partitions[str].RequestVote(&raft.RequestVoteArgs{
		Term: int(rep.Term),
		CandidateId: int(rep.CandidateId),
		LastLogIndex: int(rep.LastLogIndex),
		LastLogIterm: int(rep.LastLogIterm),
	})
	p.mu.RUnlock()

	return &api.RequestVoteReply{
		Term: int8(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (p *parts_raft)AppendEntries(ctx context.Context, rep *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error){
	str := rep.TopicName + rep.PartName
	var array []raft.LogNode
	json.Unmarshal(rep.Entries, &array)
	p.mu.RLock()
	resp := p.Partitions[str].AppendEntries(&raft.AppendEntriesArgs{
		Term: int(rep.Term),
		LeaderId: int(rep.LeaderId),
		PrevLogIndex: int(rep.PrevLogIndex),
		PrevLogIterm: int(rep.PrevLogIterm),
		LeaderCommit: int(rep.LeaderCommit),
		Entries: array,
	})
	p.mu.RUnlock()
	return &api.AppendEntriesReply{
		Success: resp.Success,
		Term: int8(resp.Term),
		Logterm: int8(resp.Logterm),
		Termfirstindex: int8(resp.Termfirstindex),
	}, nil
}

func (p *parts_raft)SnapShot(ctx context.Context, rep *api.SnapShotArgs_) (r *api.SnapShotReply, err error){
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	resp := p.Partitions[str].InstallSnapshot(&raft.SnapShotArgs{
		Term: int(rep.Term),
		LeaderId: int(rep.LeaderId),
		LastIncludedIndex: int(rep.LastIncludedIndex),
		LastIncludedTerm: int(rep.LastIncludedTerm),
		Log: rep.Log,
		Snapshot: rep.Snapshot,
	})
	p.mu.RUnlock()
	return &api.SnapShotReply{
		Term: int8(resp.Term),
	}, nil
}
