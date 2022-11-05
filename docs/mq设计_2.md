## Zookeeper

Zookeeper中将保存着每个Broker的信息，和Topic-Partition的信息，如图：


## Producter

生产者通过Topic和Partition的name查询Zookeeper获得需要将信息发送到那个broker，通过Push操作将信息发送到broker，信息的内容包括Topic和Partition的name;

生产者需要将连接Zookeeper查询到的信息进行储存，如果再次有该Topic—Partition的信息就不需要再次查询；

若生产者发送的信息的Topic-Partition不存在，我们则认为生产者需要创建一个新的Topic或者Partition，Zookeeper会通过负载均衡将该Topic-Partition分给一Broker,Broker收到一个自己不曾有的Topic和Partition时，就会创建一个。

Partition并不能无限创建，我们设置一个默认限制：10个；超出这个限制则会创建失败；

## Consumer

消费者连接到Zookeeper查询需要信息的Topic-Partition，并连接到该Broker，

### 首次连接

创建一个客户端，设置它的状态state；根据信息中的Topic-Partition和offset信息，查询到该客户端确实订阅了该Topic和Partition，则创建一个线程，创建一个该Topic-Partition的队列，从存储中读取一块信息到该队列中。并维护一个Pingong心跳；

### 订阅 TOPIC_NIL_PTP

订阅一个点对点的情况时，

### 订阅 TOPIC_KEY_PSB

### 断开连接

通过PingPong检测到该客户端失去联系后，则修改客户端状态，其他发送协程检查此选项后关闭；

通过Sub发送信息超时后，将该协程加入超时对列，若重发后任超时则加入死信队列；（此状态可以在消费者并未断开的情况，可用于消费者战时不想处理该信息，我们可以将发送这个Topic—Partition的协程战时关闭，等待消费者通知后再从死信队列取出并开启协程发送）

### 恢复连接

在订阅Topic-Partition后会将订阅的内容存下来，当恢复连接时将重新请求Zookeeper并连接Broker；