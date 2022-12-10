## Producer 功能

* 创建Topic

* 创建Partition

* 向Topic-Partition生产信息

* 设置Partition的状态，选择高可用的集中模式

## NewProducer
在程序中加入 ClyMQ/client/clients 包，使用 NewProducer 创建一个生产者

```
Producer, err := client3.NewProducer(zkbroker, name)
```
* zkbroker 是zkserver的IP和端口，producer需要连接到zkserver

* name 是producer的唯一ID， 需要手动设置， 注意：name是唯一标识

## 创建Topic
调用 CreateTopic 函数，该函数将调用RPC到zkserver，通过zkserver在zookeeper上创建topic节点

```
err := producer.CreateTopic("phone_number")
```

* 传入Topic的名字

## 创建Partition
调用 CreatePart 函数，该函数将调用RPC到zkserver，通过zkserver在zookeeper上创建partition节点
```
err = producer.CreatePart("phone_number", "xian")
```
* 传入 TopicName 和 PartitionName

## 设置Partition的状态
producer需要设置Partition的状态，也就是ACK机制的其中一种

* -1    采用raft机制同步主副本间的信息，当大部分副本存入信息后返回Push的RPC

* 1     采用fetch机制同步主副本间的信息，当Leader存入信息后返回Push的RPC
 
* 0     采用fetch机制同步主副本间的信息，当Broker收到信息后就返回Push的RPC

通过调用 SetPartitionState 函数， 此函数调用RPC到zkserver， zkserver将修改此Partition的状态，若state为1和0之间的转换，则不需要重新设置，当Push请求发送时会处理。当从 raft 到 fetch 转换时，需要停止raft集群，并创建 fetch 机制，不许要更换存信息的文件。当从 fetch 到 raft 转换时，需要停止fetch机制，将原本的主副本锁在的Broker组成raft集群，需要创建新的文件存信息。当从为设置状态到其他状态时需要使用负载均衡，分配3个Broker组成主副本集群来同步信息（副本均在不同的Broker中）。
```
err = producer.SetPartitionState("phone_number", "xian", -1, 3)
```
* 传入的第一个参数是 TopicName

* 传入的第二个参数是 PartitionName

* 传入的第三个参数是 ACK 即state

* 传入的第四个参数是集群中Broker的个数，目前只支持3，后续可增加

## Push 生产信息
producer通过Push函数生产信息，使用前需要设置Partition的状态，即上一步。。
```
err = producer.Push(clients.Message{
			Topic_name: "phone_number",
			Part_name:  "xian",
			Msg:        []byte(message),
		}, -1)
```
* 将信息封装成结构体，作为第一个参数

* ACK 机制的选项，需要和上一步设置的状态一致

```
type Message struct {
	Topic_name string       // TopicName
	Part_name  string       // PartitionName
	Msg        []byte       // 信息所转换的byte数组
}
```