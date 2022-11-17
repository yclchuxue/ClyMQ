## Zookeeper

### SDK

采用kitex支持的zookeeper的接口，但该SDK并不完整，需要补充， 且该SDK也是对go-zookeeper/zk的封装；

### Broker

当Broker启动时：需要连接到Zookeeper，（需要做一个负载均衡，通过检测各个broker的状况来负载），在zookeeper上的Brokers下创建临时节点（退出即销毁）。

启动时会恢复该节点下的信息，broker从zkserver拿到以存在在自己名下的资源，并创建各个topic，partition，和subscription；  //考虑是否可以不做，

#### producer push message

当有生产者生产信息时：生产者会先连接Zookeeper（改进，让生产者连接一个Broker，Broker来连接Zookeeper），查询该信息将交给谁来接收（负载均衡算法）；

当有生产者生产信息，向zkserver查询该向谁（broker）发送，zkserver将发送信息到该broker，broker检查topic和partition是否存在，不存在就创建一个，并且启动该partition接收功能（设置该partition的接收队列和文件）   //StartGetMessage

// 当Broker收到信息后：Broker中若没有该Topic或Partition则在本地创建文件夹和文件和Zookeeper上创建节点，并备注负责的Zookeeper是谁。


#### consumer sub and startToGet
当有消费者sub时，向zkserver sub

当有消费者消费信息时，向zkserver查询该向谁（broker）发送请求，zkserver判断是否订阅了该topic/partition，若没有则返回，若有，zkserver向broker发送信息，若该broker没有该订阅则创建一个，并根据file_name创建config，并负载均衡，发送信息；

当消费者消费信息时：连接到Zookeeper（Broker），查询该节点是由那个Broker负责，重新连接该Broker，进行消费；

* 对于PTP的情况，消费者将获得对该Topic的消费信息，和对各个节点的消费情况，连接到Brokers后将它们负责的Partitions的情况发送过去，并开始消费。

* 对于PSB的情况，消费者获取该Topic——Partition所在Broker节点后，将自己给出想要消费的位置进行消费。

当zkserver上的负载均衡发生改变时，需要将停止一些broker上的接受producer的信息的服务，转移到其他地方去。zkserver需要发送到该broker，停止信息，并将文件名进行修改，若有consumer在消费这个文件，则修改config中的文件名等，发送到新broker去接收信息。

当borker中一个file消费完后，将发送结束信息到consumer，consumer将再次请求zkserver去获取下一个broker的位置。

### 分片迁移

当分片发生迁移时会导致磁盘IO和网络IO暴增（文件迁移），而导致分片发生迁移的原因是由于负载均衡导致的，而负载均衡又是由于加入了新的Broker集群或者Broker的性能问题；而暴增的磁盘和网络IO会导致短时间内达不到减轻Broker压力和提高性能的目的，所以ClyMQ不进行文件的迁移，当分片发生迁移时我们将producer生产的消息放在新的Broker位置，消费者消费完原位置的信息后再转移到新位置进行消费。