## zkserver 功能

* 接收producer生产的信息，储存到磁盘并分发到订阅的consumers中

* 多个broker组成broker集群为topic提供高可用的消息服务

## NewRpcServer
在程序中导入 ClyMQ/server 和 ClyMQ/zookeeper 包，使用 NewRpcServer 创建一个zkserver。
```
    zookeeper_port := []string{"127.0.0.1:2181"}
	broker := Server.NewBrokerAndStart(zookeeper.ZkInfo{
			HostPorts: zookeeper_port,
			Timeout:   20,
			Root:      "/ClyMQ",
		}, Server.Options{
			Me:  			  index,	
			Name:             "Broker" + strconv.Itoa(index),
			Tag:              Server.BROKER,
			Broker_Host_Port: server_ports[index],
			Raft_Host_Port:   raft_ports[index],
			Zkserver_Host_Port: ":7878",
		})
```
其中 ZkInfo 是连接 zookeeper 需要的信息，Options是zkserver的一些信息，如下：
```
type ZkInfo struct {
	HostPorts []string          	//zookeeper的IP和端口信息
	Timeout   int
	Root      string            	//zookeeper中ClyMQ的位置
}


type Options struct {
	Me 				   int       	//启动broker server 时需要，这里不需要
	Name               string		//zkserver的唯一标识
	Tag                string		//该server是zkserver还是broker server的标记
	Zkserver_Host_Port string		//
	Broker_Host_Port   string		//
	Raft_Host_Port     string		//
}

```
## 详细使用方法请参考测试文件
[common.go](https://github.com/yclchuxue/ClyMQ/blob/master/test/common.go)