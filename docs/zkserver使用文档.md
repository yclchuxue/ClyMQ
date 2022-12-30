## zkserver 功能

* 为clients和brokers提供meta服务

* 动态扩容brokers时分配broker集群

* 为消息订阅发布等功能准备

## NewRpcServer
在程序中导入 ClyMQ/server 和 ClyMQ/zookeeper 包，使用 NewRpcServer 创建一个zkserver。
```
	zookeeper_port := []string{"127.0.0.1:2181"}
	zkserver := Server.NewZKServerAndStart(zookeeper.ZkInfo{
		HostPorts: zookeeper_port,
		Timeout:   20,
		Root:      "/ClyMQ",
	}, Server.Options{
		Name: "ZKServer",
		Tag:  Server.ZKBROKER,
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