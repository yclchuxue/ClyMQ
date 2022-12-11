## Consumer 的功能
* 通过Sub订阅Topic或Partition

* 通过 Pull 和 Pub 消费信息
## NewConsumer
consumer调用NewConsumer创建一个consumer，获得一个句柄，通过这个句柄来调用函数。
```
consumer, err := client3.NewConsumer(zkbroker, name, consumer_port)
```
* zkbroker 为zkserver的IP和端口， 通过连接到zkserver获取topic所在的Broker位置等；

* name 为consumer的唯一标识，需要手动设置，注意：需要唯一；

* consumer_port 为consumer的端口号，consumer将启动一个rpc server，接收Pub模式的信息；
## 启动Consumer
consumer调用 Start_server 启动Rpc server
```
go consumer.Start_server()
```
注意：需要新开启一个协程来调用此函数
## Sub 订阅信息
consumer调用 Subscription 来订阅 Topic 或 Partition， 该函数调研Sub RPC 到zkserver。
```
err := consumer.Subscription("phone_number", "北京", PTP)
```
* 第一个参数是 TopicName

* 第二个参数是 PartitionName

* 第三个参数是订阅的类型，一共有两个模式 PTP 和 PSB
    
    PTP：订阅Topic的所有分片，需要负载均衡，一个信息只被同种订阅的consumer消费一次

	PSB：订阅具体的Topic-Partition，不需要负载均衡,每个PSB开一个Part来发送信息，可重复消费
## 消费信息
消费信息有两种方式，Pub和Pull，订阅该Topic或Partition后需要先调用 StartGet 函数，该函数会发送 RPC 到 zkserver，zkserver 将查询那些Broker负责该Topic和Partition，向这些Broker发出通知，做好准备，同时返回需要向集群中请求数据的Leaders，且保证这些Broker处于在线状态；当RPC返回后consumer将连接到这些Broker上，向这些Broker发出自己的信息，这些Broker将会连接到consumer，得到一个 RPC 句柄。通过这个句柄将使用Pub模式消费信息。
```
parts, ret, err := consumer.StartGet(clients.Info{
		Topic: "phone_number",
		Part: "xian",
		Option: server.PTP_PULL,
	})
```
参数介绍：
```
type Info struct {
	Offset int64                        //需要消费的位置，PSB需要自己设置，PTP不需要设置
	Topic  string                       //TopicName
	Part   string                       //PartitionName
	Option int8                         //消费类型，PTP_PULL、PTP_PUSH、PSB_PULL、PSB_PUSH
	Cli    server_operations.Client     //Pull信息时需要此项，需要给出Partition所在的Broker的句柄，由StartGet会返回Broker的信息
	Bufs   map[int64]*api.PubRequest    
}
```
## 消费模式
* PTP模式，即consumer订阅了整个Topic的信息，且所有订阅PTP模式的consumer只能有一个消费者消费同一条信息
* PSB模式，即consumer订阅了某个具体的Partition，可以指定消费位置。
### Pub 模式
Pub模式即Broker将主动向consumer发送信息，通过Broker连接Consumer RPC  Server时获得的句柄，向consumer发送RPC，将信息发给consumer。
Pub模式需要主动在Pub函数中添加需要的代码来处理信息：
```
func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req)

	/*
		添加用户自己的处理代码
	*/

	return &api.PubResponse{Ret: true}, nil
}

```
### Pull 模式
Pull模式即consumer将主动向Broker拉去信息，通过Consumer连接Broker RPC  Server时获得的句柄，consumer发送RPC，获取信息，由consumer主动控制获取信息的量和时间。
```
info := clients.NewInfo(0, "phone_number", "xian")
info.Cli = *cli
start, end, msgs, err := consumer.Pull(info)
if err != nil {
	t.Fatal(err.Error())
}
```
consumer使用Pull拉取信息时需要传入info参数，该参数在上面已经有了介绍，可以通过GetCli函数获取client句柄。

## 具体方法
具体使用方法请参考测试；