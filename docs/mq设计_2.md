
## 基础架构

网络库 + 消息队列 + 数据持久化 + Zookeeper

### 消息队列

- 生产者生产消息 

- 消费者消费信息（消费信息后不会被删除，后期可提供日志监测功能）

- 消息队列持久化

- 分片管理消息队列，支持分布式

- 通过Zookeeper管理分片

- 由 Producer 向 broker push 消息并由 Consumer 从 broker pull 消息。

#### Broker
单个消息队列服务器，支持水平扩展。

#### Topic
每个broker拥有多个topic，topic是一个queue, topic是不同消息分类，即不同消费者所关心的事件。

#### Partition
为了提高吞吐率，将topic分成一个或多个Partition，类似于kv存储中按k进行分片管理，将不同分片放到不同集群。

### 网络库
通过网络库进行进程间通讯（可以考虑使用RPC），后期争取将自己的网络库可以支持ymq。

## 数据持久化
每个Partition创建一个文件，采取顺序读写的方式提高性能。
