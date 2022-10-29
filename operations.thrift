namespace go api

struct PushRequest {
    1: i64 producer
    2: string topic
    3: string key
    4: string message
}

struct PushResponse {
    1: bool ret
}

struct PullRequest {
    1: i64 consumer
    2: string topic
    3: string key
}

struct PullResponse {
    1: string message
}

struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool ret
}

service Server_Operations {
    PushResponse push(1: PushRequest req)
    PullResponse pull(1: PullRequest req)
    InfoResponse info(1: InfoRequest req)
}

struct PubRequest{
    1: string meg
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