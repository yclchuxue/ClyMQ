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

service Operations {
    PushResponse push(1: PushRequest req)
    PullResponse pull(1: PullRequest req)
}