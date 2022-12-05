package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/raft_operations"
	"ClyMQ/raft"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/server"
)

const (
	TIMEOUT        = 1000 * 1000
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrWrongNum    = "ErrWrongNum"
)

type Op struct {
	Cli_index string //client的唯一标识
	Cmd_index int64  //操作id号
	Ser_index int64  //Server的id
	Operate   string //这里的操作只有append
	Tpart     string //这里的shard为topic+partition
	Topic     string
	Part      string
	Num       int
	// KVS       map[string]string     //我们将返回的start直接交给partition，写入文件中
	CSM map[string]int64
	CDM map[string]int64

	Msg  []byte
	Size int8
}

type COMD struct {
	index int
	// csm     map[string]int64
	// cdm     map[string]int64

	// num     int
	The_num int
}

type SnapShot struct {
	// Kvs    []map[string]string
	Tpart string
	// topic 	string
	// part 	string
	Csm map[string]int64
	Cdm map[string]int64
	// KVSMAP map[int]int
	Apliedindex int
}

type parts_raft struct {
	mu         sync.RWMutex
	srv_raft   server.Server
	Partitions map[string]*raft.Raft
	Leaders    map[string]bool

	me      int
	appench chan info
	applyCh chan raft.ApplyMsg

	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	Add chan COMD

			//part    producer
	CSM map[string]map[string]int64
	CDM map[string]map[string]int64

	ChanComd map[int]COMD //管道getkvs的消息队列

	//多raft,则需要多applyindex
	applyindexs map[string]int
	// rpcindex   int
	Now_Num int
	// check   bool
}

func NewParts_Raft() *parts_raft {
	return &parts_raft{
		mu: sync.RWMutex{},
	}
}

func (p *parts_raft) make(name string, opts []server.Option, appench chan info) {

	p.appench = appench
	p.applyCh = make(chan raft.ApplyMsg)
	p.Add = make(chan COMD)

	p.CDM = make(map[string]map[string]int64)
	p.CSM = make(map[string]map[string]int64)
	p.Partitions = make(map[string]*raft.Raft)
	p.applyindexs = make(map[string]int)
	p.Leaders = make(map[string]bool)

	// DEBUG(dLog, "this raft host port is %v\n", host_port)
	// addr, _ := net.ResolveIPAddr("tcp", host_port)
	// var opts []server.Option
	// opts = append(opts, server.WithServiceAddr(addr))
	// DEBUG(dLog, "the opt %v\n", opts)
	srv_raft := raft_operations.NewServer(p, opts...)
	p.srv_raft = srv_raft

	err := srv_raft.Run()
	if err != nil {
		DEBUG(dError, "the raft run fail %v\n", err.Error())
	}
}

func (p *parts_raft) Append(in info) (string, error) {
	// Your code here.
	str := in.topic_name + in.part_name
	DEBUG(dLeader, "S%d <-- C%v putappend message(%v) topic_partition(%v)\n", p.me, in.producer, in.cmdindex, str)

	p.mu.Lock()
	//检查当前partition是否接收信息
	_, ok := p.Partitions[str]
	if !ok {
		ret := "this partition is not in this broker"
		DEBUG(dLog, "this partition(%v) is not in this broker\n", str)
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ret, errors.New(str)
	}
	_, isLeader := p.Partitions[str].GetState()

	if !isLeader {
		DEBUG(dLog, "S%d this is not leader\n", p.me)
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrWrongLeader, nil
	}

	if p.applyindexs[str] == 0 {
		DEBUG(dLog, "S%d the snap not applied applyindex is %v\n", p.me, p.applyindexs[str])
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrTimeOut, nil
	}

	in1, okk1 := p.CDM[str][in.producer]
	if okk1 && in1 == in.cmdindex {
		// DEBUG(dLeader, "S%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d)\n", kv.me, kv.gid, args.Key, args.Value, args.Op, args.CIndex, args.OIndex)
		p.mu.Unlock()
		return OK, nil
	} else if !okk1 {
		p.CDM[str][in.producer] = 0
	}
	p.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(p.me),
		Cli_index: in.producer,
		Cmd_index: in.cmdindex,
		// Operate:   args.Op,
		Topic: in.topic_name,
		Part:  in.part_name,
		Tpart: str,
		Msg:   in.message,
		Size:  in.size,
	}

	p.mu.Lock()
	DEBUG(dLog, "S%d lock 285\n", p.me)
	in2, okk2 := p.CSM[str][in.producer]
	if !okk2 {
		p.CSM[str][in.producer] = 0
	}
	p.mu.Unlock()

	if in2 == in.cmdindex {
		_, isLeader = p.Partitions[str].GetState()
	} else {
		index, _, isLeader = p.Partitions[str].Start(O)
	}

	if !isLeader {
		return ErrWrongLeader, nil
	} else {

		for {
			select {
			case out := <-p.Add:
				p.mu.Lock()
				DEBUG(dLog, "S%d lock 312\n", p.me)
				
				p.CSM[str][in.producer] = in.cmdindex
				p.mu.Unlock()
				if index == out.index {
					return OK, nil
				} else {
					DEBUG(dLog, "S%d index != out.index pytappend %d != %d\n", p.me, index, out.index)
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				_, isLeader := p.Partitions[str].GetState()
				ret := ErrTimeOut
				p.mu.Lock()
				lastindex, ok := p.CSM[str][in.producer]
				if !ok {
					p.CSM[str][in.producer] = 0
				}
				DEBUG(dLog, "S%d lock 332\n", p.me)
				DEBUG(dLeader, "S%d time out\n", p.me)
				if !isLeader {
					ret = ErrWrongLeader
					p.CSM[str][in.producer] = lastindex
				}
				p.mu.Unlock()
				return ret, nil
			}
		}
	}

}

//some problem
func (p *parts_raft) Kill(str string) {
	atomic.StoreInt32(&p.dead, 1)
	DEBUG(dLog, "S%d kill\n", p.me)
	// p.Partitions[str].Kill()
	// Your code here, if desired.
}

func (p *parts_raft) killed() bool {
	z := atomic.LoadInt32(&p.dead)
	return z == 1
}

func (p *parts_raft) SendSnapShot(str string) {
	w := new(bytes.Buffer)
	e := raft.NewEncoder(w)
	S := SnapShot{
		Csm:   p.CSM[str],
		Cdm:   p.CDM[str],
		Tpart: str,
		// Rpcindex:    kv.rpcindex,
		Apliedindex: p.applyindexs[str],
	}
	e.Encode(S)
	DEBUG(dSnap, "S%d the size need to snap\n", p.me)
	data := w.Bytes()
	go p.Partitions[str].Snapshot(S.Apliedindex, data)
	// X, num := kv.rf.RaftSize()
	// fmt.Println("S", kv.me, "raftsize", num, "snap.lastindex.X", X)
}

func (p *parts_raft) CheckSnap() {
	// kv.mu.Lock()

	for str, raft := range p.Partitions {
		X, num := raft.RaftSize()
		DEBUG(dSnap, "S%d the size is (%v) applidindex(%v) X(%v)\n", p.me, num, p.applyindexs[str], X)
		if num >= int(float64(p.maxraftstate)) {
			if p.applyindexs[str] == 0 || p.applyindexs[str] <= X {
				// kv.mu.Unlock()
				return
			}
			p.SendSnapShot(str)
		}
	}
	// kv.mu.Unlock()
}

func (p *parts_raft) StartServer() {

	DEBUG(dSnap, "S%d parts_raft start\n", p.me)

	LOGinit()

	go func() {

		for {
			if !p.killed() {
				select {
				case m := <-p.applyCh:

					if m.CommandValid {
						start := time.Now()

						DEBUG(dLog, "S%d try lock 847\n", p.me)
						p.mu.Lock()
						DEBUG(dLog, "S%d success lock 847\n", p.me)
						ti := time.Since(start).Milliseconds()
						DEBUG(dLog2, "S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", p.me, ti)

						O := m.Command.(Op)

						DEBUG(dLog, "S%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) CDM[C%v](%v) from(%v)\n", p.me, m.CommandValid, p.applyindexs[O.Tpart], m.CommandIndex, O.Cli_index, O.Cmd_index, O.Ser_index)

						if p.applyindexs[O.Tpart]+1 == m.CommandIndex {

							if O.Cli_index == "TIMEOUT" {
								DEBUG(dLog, "S%d for TIMEOUT update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							}else if O.Cli_index == "Leader" {
								DEBUG(dLog, "S%d tPart(%v) become leader\n", p.me, O.Tpart)
								p.applyindexs[O.Tpart] = m.CommandIndex

								p.appench <- info{
									producer:   O.Cli_index,
									topic_name: O.Topic,
									part_name:  O.Part,
								}

							}else if p.CDM[O.Tpart][O.Cli_index] < O.Cmd_index {
								DEBUG(dLeader, "S%d update CDM[%v] from %v to %v update applyindex %v to %v\n", p.me, O.Cli_index, p.CDM[O.Tpart][O.Cli_index], O.Cmd_index, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex

								p.CDM[O.Tpart][O.Cli_index] = O.Cmd_index
								if O.Operate == "Append" {

									select {
									case p.Add <- COMD{index: m.CommandIndex}:
										// DEBUG(dLog, "S%d write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// DEBUG(dLog, "S%d can not write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}

									p.appench <- info{
										producer:   O.Cli_index,
										message:    O.Msg,
										topic_name: O.Topic,
										part_name:  O.Part,
										size:       O.Size,
									}

								}
							} else if p.CDM[O.Tpart][O.Cli_index] == O.Cmd_index {
								DEBUG(dLog2, "S%d this cmd had done, the log had two update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							} else {
								DEBUG(dLog2, "S%d the topic_partition(%v) producer(%v) OIndex(%v) < CDM(%v)\n", p.me, O.Tpart, O.Cli_index, O.Cmd_index, p.CDM[O.Tpart][O.Cli_index])
								p.applyindexs[O.Tpart] = m.CommandIndex
							}

						} else if p.applyindexs[O.Tpart]+1 < m.CommandIndex {
							DEBUG(dWarn, "S%d the applyindex + 1 (%v) < commandindex(%v)\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
							// kv.applyindex = m.CommandIndex
						}

						if p.maxraftstate > 0 {
							p.CheckSnap()
						}

						p.mu.Unlock()
						DEBUG(dLog, "S%d Unlock 1369\n", p.me)

						// if maxraftstate > 0 {
						// 	go kv.CheckSnap()
						// }

					} else { //read snapshot
						r := bytes.NewBuffer(m.Snapshot)
						d := raft.NewDecoder(r)
						DEBUG(dSnap, "S%d the snapshot applied\n", p.me)
						var S SnapShot
						p.mu.Lock()
						DEBUG(dLog, "S%d lock 1029\n", p.me)
						if d.Decode(&S) != nil {
							p.mu.Unlock()
							DEBUG(dLog, "S%d Unlock 1384\n", p.me)
							DEBUG(dSnap, "S%d labgob fail\n", p.me)
						} else {
							p.CDM[S.Tpart] = S.Cdm
							p.CSM[S.Tpart] = S.Csm
							// kv.config = S.Config
							// kv.rpcindex = S.Rpcindex
							// kv.check = false
							DEBUG(dSnap, "S%d recover by SnapShot update applyindex(%v) to %v\n", p.me, p.applyindexs[S.Tpart], S.Apliedindex)
							p.applyindexs[S.Tpart] = S.Apliedindex
							p.mu.Unlock()
							DEBUG(dLog, "S%d Unlock 1397\n", p.me)
						}

					}

				case <-time.After(TIMEOUT * time.Microsecond):
					O := Op{
						Ser_index: int64(p.me),
						Cli_index: "TIMEOUT",
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					DEBUG(dLog, "S%d have log time applied\n", p.me)
					p.mu.RLock()
					for str, raft := range p.Partitions {
						O.Tpart = str
						raft.Start(O)
					}
					p.mu.RUnlock()
				}
			}
		}

	}()
}

//检查或创建一个raft
//添加一个需要raft同步的partition
func (p *parts_raft) AddPart_Raft(peers []*raft_operations.Client, me int, topic_name, part_name string, appendch chan info) {

	//启动一个raft，即调用Make(), 需要提供各节点broker的 raft_clients, 和该partition的管道，
	str := topic_name + part_name
	p.mu.Lock()
	_, ok := p.Partitions[str]
	if !ok {
		per := &raft.Persister{}
		part_raft := raft.Make(peers, me, per, p.applyCh, topic_name, part_name)
		p.Partitions[str] = part_raft
	}
	p.mu.Unlock()
}

func (p *parts_raft) DeletePart_raft(TopicName, PartName string) error {
	str := TopicName + PartName

	p.mu.Lock()
	defer 	p.mu.Unlock()
	raft, ok := p.Partitions[str]
	if !ok {
		DEBUG(dError, "this tpoic-partition(%v) is not in this broker\n", str)
		return errors.New("this tpoic-partition is not in this broker")
	}else{
		raft.Kill()
		delete(p.Partitions, str)
		return nil
	}
}

func (p *parts_raft) RequestVote(ctx context.Context, rep *api.RequestVoteArgs_) (r *api.RequestVoteReply, err error) {
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	resp := p.Partitions[str].RequestVote(&raft.RequestVoteArgs{
		Term:         int(rep.Term),
		CandidateId:  int(rep.CandidateId),
		LastLogIndex: int(rep.LastLogIndex),
		LastLogIterm: int(rep.LastLogIterm),
	})
	p.mu.RUnlock()

	return &api.RequestVoteReply{
		Term:        int8(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (p *parts_raft) AppendEntries(ctx context.Context, rep *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	str := rep.TopicName + rep.PartName
	var array []raft.LogNode
	json.Unmarshal(rep.Entries, &array)
	p.mu.RLock()
	resp := p.Partitions[str].AppendEntries(&raft.AppendEntriesArgs{
		Term:         int(rep.Term),
		LeaderId:     int(rep.LeaderId),
		PrevLogIndex: int(rep.PrevLogIndex),
		PrevLogIterm: int(rep.PrevLogIterm),
		LeaderCommit: int(rep.LeaderCommit),
		Entries:      array,
	})
	p.mu.RUnlock()
	return &api.AppendEntriesReply{
		Success:        resp.Success,
		Term:           int8(resp.Term),
		Logterm:        int8(resp.Logterm),
		Termfirstindex: int8(resp.Termfirstindex),
	}, nil
}

func (p *parts_raft) SnapShot(ctx context.Context, rep *api.SnapShotArgs_) (r *api.SnapShotReply, err error) {
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	resp := p.Partitions[str].InstallSnapshot(&raft.SnapShotArgs{
		Term:              int(rep.Term),
		LeaderId:          int(rep.LeaderId),
		LastIncludedIndex: int(rep.LastIncludedIndex),
		LastIncludedTerm:  int(rep.LastIncludedTerm),
		Log:               rep.Log,
		Snapshot:          rep.Snapshot,
	})
	p.mu.RUnlock()
	return &api.SnapShotReply{
		Term: int8(resp.Term),
	}, nil
}
