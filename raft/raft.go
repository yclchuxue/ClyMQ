package raft

import (
	"ClyMQ/kitex_gen/api"
	// "ClyMQ/common"
	"ClyMQ/kitex_gen/api/raft_operations"
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	TopicName 	 string
	PartName 	 string
	BeLeader	 bool
	Leader 		 int
	Command      Op
	CommandIndex int

	// For SnapShot:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogNode struct {
	LogIndex int
	Logterm  int
	BeLeader bool
	Leader 	 int
	Log      Op
}

type Raft struct {

	topic_name 	string
	part_name 	string

	mu        sync.Mutex               // Lock to protect shared access to this peer's state
	peers     []*raft_operations.Client // RPC end points of all peers
	persister *Persister               // Object to hold this peer's persisted state
	me        int                      // this peer's index into peers[]
	dead      int32                    // set by Kill()
	currentTerm int //当前任期
	leaderId    int
	votedFor int
	// cond     *sync.Cond
	state int //follower0       candidate1         leader2
	electionRandomTimeout int
	electionElapsed       int
	log []LogNode
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	X int
	snapshot []byte
	lastTerm int
	lastIndex int
	tindex int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//Work string  		//请求类型
	Term        int //候选者的任期
	CandidateId int //候选者的编号

	LastLogIndex int

	LastLogIterm int
}

type RequestVoteReply struct {
	VoteGranted bool //投票结果,同意为true
	Term        int  //当前任期，候选者用于更新自己
}

//心跳包
type AppendEntriesArgs struct {
	Term     int //leader任期
	LeaderId int //用来follower重定向到leader

	PrevLogIndex int
	PrevLogIterm int
	Entries      []LogNode

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int //当前任期，leader用来更新自己
	Success bool

	Logterm        int
	Termfirstindex int
}

type SnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Log               Op
	Snapshot          []byte
}

type SnapShotReply struct {
	Term int
}

func Make(peers []*raft_operations.Client, me int,
	persister *Persister, applyCh chan ApplyMsg, topic_name, part_name string) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.electionElapsed = 0
	rf.mu = sync.Mutex{}
	rf.topic_name = topic_name
	rf.part_name = part_name
	rand.Seed(time.Now().UnixNano())
	rf.electionRandomTimeout = rand.Intn(200) + 300
	rf.state = 0
	// rf.cond = sync.NewCond(&rf.mu)
	rf.log = []LogNode{}
	rf.X = 0

	rf.log = append(rf.log, LogNode{
		Logterm: 0,
	})

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.tindex = 0
	startindex := rf.X

	go rf.Commited(startindex, applyCh)

	LOGinit()

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot()) //快照

	go rf.ticker()

	return rf
}

func (rf *Raft) Commited(startindex int, applyCh chan ApplyMsg) {
	rf.mu.Lock()

	DEBUG(dLog2, "S%d i = 1 MMMMMMMMMMMMMMMM\n", rf.me)
	rf.mu.Unlock()

	for !rf.killed() {

		rf.mu.Lock()
		if len(rf.log) > 0 && startindex < rf.X {
			node := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastTerm,
				SnapshotIndex: rf.lastIndex,
			}
			DEBUG(dLog2, "S%d snapshot to applymsg lastindex(%d)\n", rf.me, node.SnapshotIndex)
			startindex = rf.X
			rf.lastApplied = rf.lastIndex

			rf.mu.Unlock()

			applyCh <- node
		} else {
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		var arry []LogNode
		commit := rf.commitIndex - rf.X
		applied := rf.lastApplied - rf.X

		DEBUG(dCommit, "S%d commit(%d) applied(%d) lenlog(%d) rf.X(%d)\n", rf.me, commit, applied, len(rf.log)-1, rf.X)
		if commit > applied && applied >= 0 && commit <= len(rf.log)-1 {
			arry = rf.log[applied+1 : commit+1]
		}
		rf.mu.Unlock()
		if commit > applied {
			for _, it := range arry {

				node := ApplyMsg{
					CommandValid: true,
					CommandIndex: it.LogIndex,
					Command:      it.Log,
					BeLeader:     it.BeLeader,
				}
				if node.BeLeader {
					node.TopicName = rf.topic_name
					node.PartName = rf.part_name
					node.Leader   = it.Leader
					DEBUG(dLeader, "S%d apply beleader\n", rf.me)
				}
				DEBUG(dLog, "S%d lastapp lognode = %v\n", rf.me, node)
				rf.mu.Lock()

				rf.lastApplied++
				DEBUG(dLog, "S%d comm(%d) last(%d)\n", rf.me, commit, rf.lastApplied)
				rf.mu.Unlock()
				applyCh <- node
			}
			go rf.persist()
		}

		time.Sleep(time.Millisecond * 20)
	}
	DEBUG(dError, "S%d the commit be killed\n", rf.me)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	if rf.state == 2 {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

type Per struct {
	X        int
	Term     int
	Log      []LogNode
	VotedFor int
}

func (rf *Raft) RaftSize() (int, int) {
	rf.mu.Lock()
	Xsize := rf.X
	rf.mu.Unlock()

	//修改为log的长度,来决定快照的大小
	return Xsize, rf.persister.RaftStateSize()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := NewEncoder(w)

	var Usr Per
	rf.mu.Lock()
	Usr.X = rf.X
	Usr.Log = rf.log
	SnapShot := rf.snapshot
	Usr.Term = rf.currentTerm
	Usr.VotedFor = rf.votedFor
	e.Encode(Usr)
	rf.mu.Unlock()
	data := w.Bytes()
	// DEBUG(dPersist, "S%d Persist len(data) is %v the log[%v]\n", rf.me, len(data), Usr.Log)
	go rf.persister.SaveStateAndSnapshot(data, SnapShot)
}

func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := NewDecoder(r)

	var Usr Per
	rf.mu.Lock()
	if d.Decode(&Usr) != nil {
		DEBUG(dWarn, "S%d labgob fail\n", rf.me)
	} else {
		DEBUG(dLog, "S%d ??? Term = %d votefor(%d) read len(data)%v) log= (%v)\n", rf.me, Usr.Term, Usr.VotedFor, len(data), Usr.Log)
		// // fmt.Println("S", rf.me, "??? log", Usr.Log)
		rf.currentTerm = Usr.Term
		rf.log = Usr.Log
		rf.X = Usr.X
		rf.snapshot = snapshot
		rf.lastIndex = rf.X
		// DEBUG(dPersist, "S%d len(rf.log) is %v\n", rf.me, len(rf.log))
		rf.lastTerm = rf.log[0].Logterm
		rf.commitIndex = rf.X
		rf.lastApplied = rf.X
		// DEBUG(dLog, "S%d 恢复log lastindex(%d) lastapplied(%d) commitindex(%d)\n", rf.me, rf.lastIndex, rf.lastApplied, rf.commitIndex)
		rf.votedFor = Usr.VotedFor
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
	}
	rf.mu.Unlock()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	// fmt.Println("S", rf.me, "index", index, "rf.X", rf.X)
	le := index - rf.X
	if le <= 0 {
		rf.mu.Unlock()
		return
	}
	rf.lastIndex = index
	rf.lastTerm = rf.log[le].Logterm
	// if le < 0 {
	// 	// fmt.Println("ERROR in snapshot in leader for le")
	// }
	rf.snapshot = snapshot
	rf.log = rf.log[le:]
	// if len(rf.log) <= 0 {
	// 	// fmt.Println("ERROR in snapshot in leader for log")
	// }
	rf.X = index
	for i := range rf.peers {
		if rf.nextIndex[i]-le <= 0 {
			rf.nextIndex[i] = len(rf.log)
		} else {
			rf.nextIndex[i] = rf.nextIndex[i] - le
		}
		// DEBUG(dLog, "S%d update nextindex[%d] to %d\n", rf.me, i, rf.nextIndex[i])
		// DEBUG(dLog, "S%d the mathindex[%d] is %d\n", rf.me, i, rf.matchIndex[i])
		// if rf.matchIndex[i]-le < 0 {
		// 	rf.matchIndex[i] = 0
		// } else {
		// 	rf.matchIndex[i] = rf.matchIndex[i] - le
		// }
		// DEBUG(dLog, "S%d update mathindex[%d] to %d\n", rf.me, i, rf.matchIndex[i])
	}
	if rf.commitIndex < index {
		DEBUG(dLeader, "S%d update commitindex(%d) to (%d)\n", rf.me, rf.commitIndex, index)
		rf.commitIndex = index
	}
	rf.lastApplied = index

	// DEBUG(dLog, "S%d index(%d) logindex(%d) len(%d)AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n", rf.me, index, rf.log[len(rf.log)-1].LogIndex, len(rf.log))
	rf.mu.Unlock()
	go rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs) (reply *RequestVoteReply) {
	//待处理收到请求投票信息后是否更新超时时间

	//所有服务器和接收者的处理流程
	// DEBUG(dLog, "S%d brfore lock\n", rf.me)
	reply = &RequestVoteReply{}
	rf.mu.Lock()
	if rf.currentTerm > args.Term { //候选者任期低于自己
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		DEBUG(dVote, "S%d  vote <- %d T(%d) < cT(%d) A\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	} else if rf.currentTerm <= args.Term { //候选者任期高于自己

		if rf.currentTerm < args.Term {
			rf.state = 0
			rf.currentTerm = args.Term
			rf.votedFor = -1
			go rf.persist()
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //任期相同且未投票或者候选者和上次相同
			//if 日志至少和自己一样新
			logi := len(rf.log) - 1
			// DEBUG(dLog, "S%d the len(log) is %v\n", rf.me, logi+1)
			if args.LastLogIterm >= rf.log[logi].Logterm {
				if args.LastLogIndex-rf.X >= logi ||
					args.LastLogIterm > rf.log[logi].Logterm {
					rf.state = 0
					DEBUG(dLog, "S%d args.Term(%v)\n", rf.me, args.Term)
					DEBUG(dLog, "S%d reply.Term(%v)\n", rf.me, reply.Term)
					reply.Term = args.Term
					rf.electionElapsed = 0
					rand.Seed(time.Now().UnixNano())
					rf.electionRandomTimeout = rand.Intn(200) + 300
					rf.votedFor = args.CandidateId
					rf.leaderId = -1
					DEBUG(dVote, "S%d  vote <- %d T(%d) = LastlogT(%d) logi(%d) lastlogindex(%d)\n", rf.me, args.CandidateId, rf.log[logi].Logterm, args.LastLogIterm, logi, args.LastLogIndex)
					reply.VoteGranted = true

				} else {
					DEBUG(dVote, "S%d  vote <- %d not lastlogIn(%d) < rf.logIn(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIndex, logi, rf.votedFor)

					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				DEBUG(dVote, "S%d  vote <- %d not logT(%d) < rf.logT(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIterm, rf.log[logi].Logterm, rf.votedFor)

				reply.VoteGranted = false
				reply.Term = args.Term
			}

		} else {

			DEBUG(dVote, "S%d  vote <- %d not T(%d) = cT(%d) vf(%d)\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

			reply.VoteGranted = false
			reply.Term = args.Term
		}
	}
	go rf.persist()
	rf.mu.Unlock()
	return reply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs) (reply *AppendEntriesReply) {
	// go rf.persist()
	reply = &AppendEntriesReply{}
	rf.mu.Lock()
	if len(args.Entries) != 0 {
		DEBUG(dLeader, "S%d  app <- %d T(%d) cT(%d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		DEBUG(dLeader, "S%d  heart <- %d T(%d) cT(%d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	}

	if args.Term >= rf.currentTerm { //收到心跳包的任期不低于当前任期

		rf.electionElapsed = 0
		rand.Seed(time.Now().UnixNano())
		rf.electionRandomTimeout = rand.Intn(200) + 300

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}

		rf.state = 0
		rf.currentTerm = args.Term
		// DEBUG(dLog, "S%d YYYY\n", rf.me)
		if rf.leaderId != args.LeaderId {
			DEBUG(dLog, "S%d be follower\n", rf.me)
		}
		rf.leaderId = args.LeaderId

		logs := args.Entries

		if len(rf.log)-1 >= args.PrevLogIndex-rf.X && args.PrevLogIndex-rf.X >= 0 {
			// DEBUG(dLeader, "S%d PreT(%d) LT(%d)\n", rf.me, args.PrevLogIterm, rf.log[args.PrevLogIndex-rf.X].Logterm)
			if args.PrevLogIterm == rf.log[args.PrevLogIndex-rf.X].Logterm {

				index := args.PrevLogIndex + 1 - rf.X

				for i, val := range logs {

					if len(rf.log)-1 >= index {
						// DEBUG(dLog, "S%d mat(%d) index(%d) len(%d)\n", rf.me, len(rf.log)-1, index, len(rf.log))
						if rf.log[index].Logterm == val.Logterm {
							index++
						} else {
							rf.log = rf.log[:index]
							// DEBUG(dLog, "S%d CCCCCCCCCCCCCCCCC\n", rf.me)
							//rf.matchIndex[rf.me] = index - 1
							rf.log = append(rf.log, logs[i:]...)
							DEBUG(dLog, "S%d A success + log(%v)\n", rf.me, logs[i:])
							//rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
							rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
							index++
							break
						}
					} else {
						rf.log = append(rf.log, logs[i:]...)
						DEBUG(dLog, "S%d B success + log(%v)\n", rf.me, logs[i:])
						//rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
						rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
						index++
						break
					}
				}
				reply.Success = true

				if args.LeaderCommit > rf.commitIndex {
					if rf.log[len(rf.log)-1].LogIndex <= args.LeaderCommit {
						rf.commitIndex = rf.log[len(rf.log)-1].LogIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					DEBUG(dCommit, "S%d update commit(%d)\n", rf.me, rf.commitIndex)

				}

			} else {

				reply.Logterm = rf.log[args.PrevLogIndex-rf.X].Logterm //冲突日志任期
				i := args.PrevLogIndex - rf.X
				for rf.log[i].Logterm == reply.Logterm {
					if i <= 1 {
						// DEBUG(dWarn, "S%d j = %d\n", rf.me, i)
						break
					}
					i--
				}

				reply.Termfirstindex = rf.log[i].LogIndex + 1 //reply.Logterm任期内的第一条日志
				// DEBUG(dLog, "S%d DDDDDDDDDDDDDDDDDDD the len(log) is %v\n", rf.me, len(rf.log))
				if args.PrevLogIndex-rf.X != 0 {
					rf.log = rf.log[:args.PrevLogIndex-rf.X] //匹配失败，删除该日志条目及其后面的日志
				} else {
					DEBUG(dSnap, "S%d the log behind the leader need snapshot\n", rf.me)
				}
				reply.Success = false
				DEBUG(dLeader, "S%d AAA fail len(log) is %v\n", rf.me, len(rf.log))
			}
			go rf.persist()
		} else { //不匹配
			if len(rf.log) < 1 {
				reply.Termfirstindex = 0 //reply.Logterm任期内的第一条日志
			} else {
				reply.Logterm = rf.log[len(rf.log)-1].Logterm //最新日志条目的任期
				i := len(rf.log) - 1
				for rf.log[i].Logterm == reply.Logterm {
					if i <= 1 {
						// DEBUG(dWarn, "S%d i = %d\n", rf.me, i)
						reply.Termfirstindex = rf.log[i].LogIndex
						break
					}
					i--
					reply.Termfirstindex = rf.log[i].LogIndex + 1 //reply.Logterm任期内的第一条日志
				}
			}
			reply.Success = false
			DEBUG(dLeader, "S%d BBB fail logi(%d) pre(%d) TI(%d)\n", rf.me, len(rf.log)-1, args.PrevLogIndex-rf.X, reply.Termfirstindex)
		}
		reply.Term = args.Term
	} else { //args.term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		DEBUG(dLeader, "S%d CCC fail\n", rf.me)
		reply.Logterm = 0
	}
	go rf.persist()
	rf.mu.Unlock()
	return reply
}

func (rf *Raft) InstallSnapshot(args *SnapShotArgs) ( reply *SnapShotReply) {

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm && args.LeaderId == rf.leaderId && args.LastIncludedIndex > rf.X {
		// DEBUG(dSnap, "S%d the len(log) is %v\n", rf.me, len(rf.log))
		if rf.log[len(rf.log)-1].LogIndex < args.LastIncludedIndex || rf.X > args.LastIncludedIndex {
			array := []LogNode{
				{
					LogIndex: args.LastIncludedIndex,
					Logterm:  args.LastIncludedTerm,
					Log:      args.Log,
				},
			}
			rf.log = array
			// if len(rf.log) <= 0 {
			// 	// fmt.Println("ERROR in snapshot in follower log < snap")
			// }
			rf.X = args.LastIncludedIndex
			rf.matchIndex[rf.me] = rf.X
		} else {
			le := args.LastIncludedIndex - rf.X
			rf.log = rf.log[le:]
			// if len(rf.log) <= 0 {
			// 	// fmt.Println("ERROR in snapshot in follower log > snap")
			// }
			rf.X = args.LastIncludedIndex
			for i := range rf.peers {
				//rf.matchIndex[i] = rf.matchIndex[i] - le
				if rf.nextIndex[i]-le <= 0 {
					rf.nextIndex[i] = len(rf.log)
				} else {
					rf.nextIndex[i] = rf.nextIndex[i] - le
				}
				// DEBUG(dLog, "S%d update next[%d] to %d\n", rf.me, i, rf.nextIndex[i])
			}
			rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
		}
		rf.lastTerm = args.LastIncludedTerm
		rf.lastIndex = args.LastIncludedIndex
		if rf.commitIndex < rf.lastIndex {
			DEBUG(dLeader, "S%d update commitindex(%d) to (%d)\n", rf.me, rf.commitIndex, rf.lastIndex)
			rf.commitIndex = rf.lastIndex
		}
		rf.snapshot = args.Snapshot
		// if rf.lastApplied < rf.lastIndex {
		rf.lastApplied = rf.lastIndex
		// }
		go rf.persist()
		// DEBUG(dLog2, "S%d aegs.Term(%d) CT(%d)\n", rf.me, args.Term, rf.currentTerm)
		DEBUG(dLog2, "S%d <- snapshot by(%d) index(%d) logindex(%d) len1(%d)AAAAAAAAAAAAAAAAAAAAAA lensnapshot(%d)\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.log[len(rf.log)-1].LogIndex, len(rf.log), len(args.Snapshot))
	} else {
		DEBUG(dLog2, "S%d <- snapshot but term(%d) < cT(%d) or leaderid(%d) != args.LeaderID(%d)\n", rf.me, args.Term, rf.currentTerm, rf.leaderId, args.LeaderId)
	}
	rf.mu.Unlock()
	return reply
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapShotArgs) (*SnapShotReply, bool) {
	resp, err := (*(rf.peers[server])).SnapShot(context.Background(), &api.SnapShotArgs_{
		Term: int8(args.Term),
		LeaderId: int8(args.LeaderId),
		LastIncludedIndex: int8(args.LastIncludedIndex),
		LastIncludedTerm: int8(args.LastIncludedTerm),
		// Log: args.Log,
		Snapshot: args.Snapshot,
		TopicName: rf.topic_name,
		PartName: rf.part_name,
	})
	if err != nil {
		return nil, false
	}
	return &SnapShotReply{
		Term: int(resp.Term),
	}, true
}

//发送心跳包
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) ( *AppendEntriesReply, bool) {
	data_array,_ := json.Marshal(args.Entries)
	resp, err := (*(rf.peers[server])).AppendEntries(context.Background(), &api.AppendEntriesArgs_{
		Term: int8(args.Term),
		LeaderId: int8(args.LeaderId),
		PrevLogIndex: int8(args.PrevLogIndex),
		PrevLogIterm: int8(args.PrevLogIterm),
		LeaderCommit: int8(args.LeaderCommit),
		Entries: data_array,
		TopicName: rf.topic_name,
		PartName: rf.part_name,
	})
	if err != nil {
		return nil, false
	}
	return &AppendEntriesReply{
		Success: resp.Success,
		Term: int(resp.Term),
		Logterm: int(resp.Logterm),
		Termfirstindex: int(resp.Termfirstindex),
	}, true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) ( *RequestVoteReply, bool) {
	resp, err := (*(rf.peers[server])).RequestVote(context.Background(), &api.RequestVoteArgs_{
		Term: int8(args.Term),
		CandidateId: int8(args.CandidateId),
		LastLogIndex: int8(args.LastLogIndex),
		LastLogIterm: int8(args.LastLogIterm),
		TopicName: rf.topic_name,
		PartName: rf.part_name,
	})
	if err != nil {
		return nil, false
	}
	return &RequestVoteReply{
		Term: int(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, true
}

func (rf *Raft) Start(command Op, beleader bool, leader int) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	if !rf.killed() {

		rf.mu.Lock()
		if rf.state == 2 {
			isLeader = true

			com := LogNode{
				Logterm:  rf.currentTerm,
				Log:      command,
				BeLeader: beleader,
				Leader:   leader,
				LogIndex: len(rf.log) + rf.X,
			}
			rf.log = append(rf.log, com)
			rf.matchIndex[rf.me]++
			term = rf.currentTerm
			index = com.LogIndex
			DEBUG(dLog, "S%d have log %v\n", rf.me, com)
			// // fmt.Println("S", rf.me, "have a log command", command)
			// // fmt.Println("S", rf.me, "the log", rf.log)

			go rf.persist()
			// DEBUG(dLog, "S%d %v\n", rf.me, com)
			rf.electionElapsed = 0
			go rf.appendentries(rf.currentTerm)
		}
		//else{
		//DEBUG(dLog,"S%d is not leader\n", rf.me)
		//}
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

func (rf *Raft) Find(in int) interface{} {
	var logs []LogNode
	rf.mu.Lock()
	logs = append(logs, rf.log...)
	rf.mu.Unlock()
	for _, L := range logs {
		if L.LogIndex == in {
			return L.Log
		}
	}
	return nil
}

func (rf *Raft) appendentries(term int) {

	var wg sync.WaitGroup
	rf.mu.Lock()
	index := len(rf.log) - 1
	commit := rf.log[index].LogIndex
	t := rf.log[index].Logterm
	le := len(rf.peers)
	rf.mu.Unlock()
	wg.Add(le - 1)

	//start := time.Now()

	for it := range rf.peers {
		if it != rf.me {
			go func(it int, term int) {
				//for {
				args := AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = rf.me
				rf.mu.Lock()
				// if rf.nextIndex[it]-1 > index {
				// 	// fmt.Println("AAAAAAAAAAAAAAAAA", rf.nextIndex[it])
				// }
				if rf.tindex != rf.X || index != len(rf.log)-1 {
					rf.tindex = rf.X
					DEBUG(dLeader, "S%d appendentries error to exit because tindex or loglen changed\n", rf.me)
					rf.mu.Unlock()
					wg.Done()
					return
				}
				if index == len(rf.log)-1 {
					if commit-rf.X >= 0 && commit-rf.X < len(rf.log)-1 && rf.log[commit-rf.X].LogIndex != commit {
						DEBUG(dLeader, "S%d appendentries error to exit because log changed 2\n", rf.me)
						rf.mu.Unlock()
						wg.Done()
						return
					}
					// // fmt.Println("BBBBBBBBBBBBBBBBB")
				}
				if rf.currentTerm != term || rf.state != 2 {
					DEBUG(dLeader, "S%d appendentries error to exit because term changed or not leader CT(%d) T(%d) Status(%d)\n", rf.me, rf.currentTerm, term, rf.state)
					rf.mu.Unlock()
					wg.Done()
					return
				}
				// DEBUG(dLeader, "S%d rf.nextindex[%d] = %d\n", rf.me, it, rf.nextIndex[it])
				args.PrevLogIndex = rf.log[rf.nextIndex[it]-1].LogIndex
				// DEBUG(dLeader, "S%d index(%d)  Pre(%d) len(%d)\n", rf.me, index, args.PrevLogIndex, len(rf.log)-1)
				DEBUG(dLeader, "S%d app -> %d next(%d) index(%d) neT(%d) cT(%d)\n", rf.me, it, rf.nextIndex[it], index, rf.log[args.PrevLogIndex-rf.X].Logterm, term)

				args.PrevLogIterm = rf.log[rf.nextIndex[it]-1].Logterm

				if len(rf.log)-1 >= rf.nextIndex[it] && rf.log[rf.nextIndex[it]].LogIndex < commit+1 {
					nums := rf.log[rf.log[rf.nextIndex[it]].LogIndex-rf.X : commit-rf.X+1]
					args.Entries = append(args.Entries, nums...)
				}

				//附加commitIndex，让follower应用日志
				args.LeaderCommit = rf.commitIndex

				iter := it
				rf.mu.Unlock()
				// reply := AppendEntriesReply{}

				reply, ok := rf.sendAppendEntries(iter, &args)

				// start := time.Now()
				if ok {
					rf.mu.Lock()
					if reply.Success {

						successnum := 0

						DEBUG(dLog, "S%d mathindex[%v] from %v to %v\n", rf.me, it, rf.matchIndex[it], commit)
						rf.matchIndex[it] = commit
						//统计复制成功的个数，超过半数就提交（修改commitindex）

						if rf.tindex == rf.X {
							if commit-rf.X+1 <= 0 {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to 1 success because com-x+1(%v)\n", rf.me, it, rf.nextIndex[it], commit-rf.X+1)
								rf.nextIndex[it] = 1
							} else {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success\n", rf.me, it, rf.nextIndex[it], commit-rf.X+1)
								rf.nextIndex[it] = commit - rf.X + 1 //index + 1
							}
						} else {
							if commit >= rf.X {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success?\n", rf.me, it, rf.nextIndex[it], commit-rf.X+1)
								rf.nextIndex[it] = commit - rf.X + 1
							} else {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success 1\n", rf.me, it, rf.nextIndex[it], 1)
								rf.nextIndex[it] = 1
							}
							rf.tindex = rf.X
						}
						DEBUG(dLog, "S%d index(%v) matchindex(%v)\n", rf.me, index, rf.matchIndex)
						for _, in := range rf.matchIndex {
							if in >= commit {
								successnum++
							}
						}
						DEBUG(dLog, "S%d successnum(%v) com-rf.X(%v) rf.CT(%v) t(%v)\n", rf.me, successnum, rf.commitIndex-rf.X, rf.currentTerm, t)
						if successnum > le/2 && index > rf.commitIndex-rf.X && rf.currentTerm == t {
							DEBUG(dLog, "S%d sum(%d) ban(%d)\n", rf.me, successnum, le/2)
							DEBUG(dCommit, "S%d new commit(%d) and applied\n", rf.me, index)
							rf.commitIndex = commit
						}

					} else {
						if reply.Term > rf.currentTerm {
							DEBUG(dLeader, "S%d  app be %d's follower T(%d)\n", rf.me, -1, reply.Term)
							rf.state = 0
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.leaderId = -1 //int(Id)
							go rf.persist()
							rf.electionElapsed = 0
							rand.Seed(time.Now().UnixNano())
							rf.electionRandomTimeout = rand.Intn(200) + 300
						} else if rf.state == 2 {
							if reply.Logterm >= 0 {
								DEBUG(dLog, "S%d to %d 匹配失败 tfi(%d)\n", rf.me, it, reply.Termfirstindex)

								//跳过整个冲突任期----可能需要判断该index是否存在
								if reply.Termfirstindex < rf.X { //跟随者日志index小于leader的第一条日志index，发快照同步。
									DEBUG(dLog2, "S%d send snapShot to %d\n", rf.me, it)
									go rf.SendSnapshot(rf.currentTerm, it)
								} else if reply.Termfirstindex-rf.X > 1 {
									DEBUG(dLeader, "S%d update nextindex[%d](%d) to X(%d) > 1\n", rf.me, it, rf.nextIndex[it], reply.Termfirstindex-rf.X)
									rf.nextIndex[it] = reply.Termfirstindex - rf.X
									if rf.nextIndex[it] > len(rf.log) {
										DEBUG(dLeader, "S%d update nextindex[%d](%d) to %v> 1\n", rf.me, it, rf.nextIndex[it], len(rf.log))
										rf.nextIndex[it] = len(rf.log)
									}
								} else {
									DEBUG(dLog, "S%d update nextindex[%d](%d) to %d <= 1\n", rf.me, it, rf.nextIndex[it], 1)
									rf.nextIndex[it] = 1
								}
							} else {
								DEBUG(dLog, "S%d reply.logterm == 0\n", rf.me)
							}
						}
					}
					rf.mu.Unlock()
				} else {
					DEBUG(dLog, "S%d -> %d app fail\n", rf.me, it)
				}

				wg.Done()
			}(it, term)
		}
	}

	wg.Wait()
}

func (rf *Raft) SendSnapshot(term, it int) {

	args := SnapShotArgs{}
	// reply := SnapShotReply{}
	rf.mu.Lock()
	args.Term = term
	args.LastIncludedIndex = rf.lastIndex
	args.LastIncludedTerm = rf.lastTerm
	args.Log = rf.log[0].Log
	args.Snapshot = rf.snapshot
	args.LeaderId = rf.me
	rf.mu.Unlock()
	reply, ok := rf.sendInstallSnapshot(it, &args)

	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.state = 0
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1 //int(Id)
			go rf.persist()
			rf.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(200) + 300
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestvotes(term int) {

	rf.mu.Lock()
	truenum := int64(1)
	peers := len(rf.peers)
	rf.votedFor = rf.me
	DEBUG(dVote, "S%d  vote vf(%d) to own wg is %v\n", rf.me, rf.votedFor, len(rf.peers)-1)
	var wg sync.WaitGroup

	wg.Add(len(rf.peers) - 1)

	rf.mu.Unlock()

	for it := range rf.peers {
		if it != rf.me {

			go func(it int, term int) {
				args := RequestVoteArgs{}
				// reply := RequestVoteReply{}
				args.CandidateId = rf.me
				args.Term = term
				rf.mu.Lock()
				index := len(rf.log) - 1
				args.LastLogIndex = rf.log[index].LogIndex
				args.LastLogIterm = rf.log[index].Logterm

				rf.mu.Unlock()

				DEBUG(dVote, "S%d  vote -> %d cT(%d)\n", rf.me, it, term)
				reply, ok := rf.sendRequestVote(it, &args) //发起投票

				if ok {
					rf.mu.Lock()
					if term != rf.currentTerm {

						DEBUG(dVote, "S%d  vote tT(%d) != cT(%d)\n", rf.me, term, rf.currentTerm)

					} else if rf.state == 1 {

						//处理收到的票数
						if reply.VoteGranted && reply.Term == term {
							atomic.AddInt64(&truenum, 1)
						}

						if atomic.LoadInt64(&truenum) > int64(peers/2) { //票数过半

							rf.state = 2
							rf.electionElapsed = 0
							rf.electionRandomTimeout = 90

							DEBUG(dVote, "S%d  have %d votes T(%d) cT(%d) %d B\n", rf.me, truenum, term, rf.currentTerm, peers/2)
							rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex

							for i := 0; i < len(rf.peers); i++ {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d (len(log))\n", rf.me, it, rf.nextIndex[it], len(rf.log))
								rf.nextIndex[i] = len(rf.log)
								if i != rf.me {
									rf.matchIndex[i] = 0
								}
							}

							//成为Leader将修改zookeeper上的信息
							go rf.Start(Op{
								Cli_name: "Leader",
								Topic: rf.topic_name,
								Tpart: rf.topic_name+rf.part_name,
								Part: rf.part_name,
							}, true, rf.me)

							go rf.appendentries(rf.currentTerm)
							DEBUG(dLeader, "S%d  be Leader B\n", rf.me)

						}

						if reply.Term > rf.currentTerm {
							rf.state = 0
							rf.currentTerm = reply.Term
							rf.leaderId = -1
							rf.votedFor = -1
							rf.electionElapsed = 0
							rand.Seed(time.Now().UnixNano())
							rf.electionRandomTimeout = rand.Intn(200) + 300
							DEBUG(dVote, "S%d vote T(%d) > cT(%d) be -1's follower vf(%d)\n", rf.me, term, rf.currentTerm, rf.votedFor)
						}
						go rf.persist()
					}
					rf.mu.Unlock()
				} else {
					DEBUG(dVote, "S%d vote -> %d fail\n", rf.me, it)
				}

				DEBUG(dLog, "S%d Done it is %v\n", rf.me, it)
				wg.Done()
			}(it, term)

		}
	}

	wg.Wait()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	//var start time.Time
	//start = time.Now()
	for !rf.killed() {

		rf.mu.Lock()

		if rf.electionElapsed >= rf.electionRandomTimeout {
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(200) + 300
			rf.electionElapsed = 0
			if rf.state == 2 {
				rf.electionRandomTimeout = 90
				go rf.persist()
				go rf.appendentries(rf.currentTerm)
			} else {
				rf.currentTerm++
				rf.state = 1
				rf.votedFor = -1
				go rf.persist()
				go rf.requestvotes(rf.currentTerm)
			}
		}

		rf.electionElapsed++

		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
		//ti := time.Since(start).Milliseconds()
		//log.Printf("S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", rf.me, ti)
	}
}


type Op struct {
	Cli_name string //client的唯一标识
	Cmd_index int64  //操作id号
	Ser_index int64  //Server的id
	Operate   string //这里的操作只有append
	Tpart     string //这里的shard未topic+partition
	Topic     string
	Part      string
	Num       int
	// KVS       map[string]string     //我们将返回的start直接交给partition，写入文件中
	// CSM map[string]int64
	// CDM map[string]int64

	Msg  []byte
	Size int8
}


//节点恢复
//func (rf *Raft)UpdatePeers()