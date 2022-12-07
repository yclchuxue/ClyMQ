namespace go api

struct RequestVoteArgs{
    1:  i8      Term
    2:  i8      CandidateId
    3:  i8      LastLogIndex
    4:	i8      LastLogIterm
    5:  string  TopicName
    6:  string  PartName
}

struct RequestVoteReply{
    1:  bool    VoteGranted
    2:  i8      Term
}

struct AppendEntriesArgs {
    1:  i8      Term
    2:  i8      LeaderId
    3:  i8      PrevLogIndex
    4:  i8      PrevLogIterm
    5:  i8      LeaderCommit
    6:  binary  Entries
    7:  string  TopicName
    8:  string  PartName
}

struct AppendEntriesReply{
    1:  bool    Success
    2:  i8      Term
    3:  i8      Logterm
    4:  i8      Termfirstindex
}

struct SnapShotArgs {
    1:  i8      Term
    2:  i8      LeaderId
    3:  i8      LastIncludedIndex
    4:  i8	    LastIncludedTerm
    5:  binary	Log
    6:  binary	Snapshot
    7:  string  TopicName
    8:  string  PartName
}

struct SnapShotReply {
    1:  i8      Term
}

struct PingPongArgs {
    1: bool ping
}

struct PingPongReply{
    1: bool pong
}

service Raft_Operations{
    RequestVoteReply    RequestVote(1: RequestVoteArgs rep)
    AppendEntriesReply  AppendEntries(1: AppendEntriesArgs rep)
    SnapShotReply       SnapShot(1:  SnapShotArgs rep)
    PingPongReply    pingpongtest(1: PingPongArgs  req)
}