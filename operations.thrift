namespace go api

struct PushRequest {
    1: string producer
    2: string topic
    3: string key
    4: string message
}

struct PushResponse {
    1: bool     ret
    2: string   err   //用来回复该partition已经不在这个broker接收
}

//暂时不使用该功能
struct PullRequest {
    1: string consumer
    2: string topic
    3: string key
}

struct PullResponse {
    1: string message
}

//consmer发送自己的host和ip,使broker连接上自己
struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool ret
}

//consumer准备开始接收信息
struct InfoGetRequest {
    1: string   cli_name
    2: string   topic_name
    3: string   part_name
    4: i64      offset
    5: i8   option
}

struct InfoGetResponse {
    1: bool  ret
}

//设置某个partition接受信息的文件和队列
struct StartGetMessageRequest {
    1:  string topic_name
    2:  string part_name
    3:  string file_name
}

struct StartGetMessageResponse {
    1: bool    ret
}

//关闭某个partition，停止接收信息，
struct CloseGetMessageRequest{
    1:  string topic_name
    2:  string part_name
    3:  string file_name
    4:  string new_name
}

struct CloseGetMessageResponse{
    1:  bool    ret
}

//zkserver
struct PrepareAcceptRequest{
    1:  string  topic_name
    2:  string  part_name
    3:  string  file_name
}

struct PrepareAcceptResponse{
    1:  bool    ret
    2:  string  err
}

struct CloseAcceptRequest{
    1:  string  topic_name
    2:  string  part_name
    3:  string  oldfilename
    4:  string  newfilename
}

struct CloseAcceptResponse{
    1:  bool    ret
    2:  i64     startindex
    3:  i64     endindex
}

struct PrepareSendRequest{
    1:  string  topic_name
    2:  string  part_name
    3:  string  file_name
    4:  i8      option
    5:  i64     offset
}

struct PrepareSendResponse{
    1:  bool    ret
    2:  string  err   //若已经准备好“had_done”
}

service Server_Operations {
    PushResponse push(1: PushRequest req)               //producer used
    PullResponse pull(1: PullRequest req)               //
    InfoResponse ConInfo(1: InfoRequest req)            //consumer used
    InfoGetResponse StarttoGet(1: InfoGetRequest req)   //consumer used

    //zkserver used this rpc to request broker server
    PrepareAcceptResponse   PrepareAccept(1: PrepareAcceptRequest req)
    CloseAcceptResponse     CloseAccept(1: CloseAcceptRequest req)
    PrepareSendResponse     PrepareSend(1: PrepareSendRequest req)
}

//broker server 将信息发送到zkserver， zkserver连接上broker server
struct BroInfoRequest {
    1: string broker_name
    2: string broker_host_port
}

struct BroInfoResponse {
    1: bool ret
}

//broker 请求恢复缓存信息
struct BroGetConfigRequest {
    1:  binary  propertyinfo
}

struct BroGetConfigResponse {
    1:  bool    ret
    2:  binary  brokerinfo
}

//producer 请求zkserver信息该发送到那个broker上
struct ProGetBrokRequest {
    1:  string  topic_name
    2:  string  part_name
}

struct ProGetBrokResponse {
    1:  bool    ret
    2:  string  broker_host_port
}

//consumer 请求zkserver 
struct ConStartGetBrokRequest {
    1:  string  cli_name
    2:  string  topic_name
    3:  string  part_name
    4:  i8      option
    5:  i64     index
}
        //返回brokers
struct ConStartGetBrokResponse   {
    1:  bool    ret
    2:  i64     size
    3:  binary  parts
//    3:  binary  broks
}

struct CreateTopicRequest {
    1:  string  topic_name
}

struct CreateTopicResponse {
    1:  bool    ret
    2:  string  err
}

struct CreatePartRequest {
    1:  string  topic_name
    2:  string  part_name
}

struct CreatePartResponse {
    1:  bool    ret
    2:  string  err
}

//consumer订阅topic或partition，由zkserver处理
struct SubRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct SubResponse {
    1: bool ret
}

service ZkServer_Operations {
    //producer和consumer
    SubResponse  Sub(1:  SubRequest  req)               //consumer used
    CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
    CreatePartResponse  CreatePart(1: CreatePartRequest req)
    ProGetBrokResponse ProGetBroker(1:  ProGetBrokRequest req)
    ConStartGetBrokResponse ConStartGetBroker(1:  ConStartGetBrokRequest req)

    //broker
    BroInfoResponse BroInfo(1: BroInfoRequest req)  //broker 发送info让zkserver连接broker
    //broker更新topic-partition的offset

    //broker 用于恢复缓存的，暂时不使用
    BroGetConfigResponse BroGetConfig(1:    BroGetConfigRequest req)
}

struct PubRequest{
    1: string topic_name
    2: string part_name
    3: i64    start_index
    4: i64    end_index
    5: binary msg
}

struct PubResponse{
    1: bool ret
}

struct PingPongRequest {
    1: bool ping
}

struct PingPongResponse{
    1: bool pong
}

service Client_Operations {
    PubResponse pub(1: PubRequest req)
    PingPongResponse pingpong(1: PingPongRequest req)
}