### ClyMQ
ClyMQ是一款基于x86/x86_64架构，运行于Linux操作系统的，由Go语言开发的分布式的面向消息的中间件，能够实时储存和分发数据，在程序中能简单搭建起来，你可以毫不费力的构建分布式和可扩展的客户端-服务器应用程序。

1.  支持多种订阅模式，用户可更具不同场合选择消息的消费模式。

2.  支持分布式和动态扩容，保证高可用和动态的扩容。

3.  多中同步副本方式，通过ack机制根据需求选择适合的同步方式。

4.  采用SSTable标记索引位置，加速消息索引查询。

5.  使用Zookeeper存储meta数据，简化ClyMQ数据结构。

6.  顺序读写磁盘，提高数据的读写速率。

### 使用说明
#### 安装
ClyMQ使用了cloudwego的kitex，所以需要先安装kitex。
1. 确保 GOPATH 环境变量已经被正确地定义（例如 export GOPATH=~/go）并且将$GOPATH/bin添加到 PATH 环境变量之中（例如 export PATH=$GOPATH/bin:$PATH）；请勿将 GOPATH 设置为当前用户没有读写权限的目录
2. 安装 kitex：go install github.com/cloudwego/kitex/tool/cmd/kitex@latest
3. 安装 thriftgo：go install github.com/cloudwego/thriftgo@latest

若安装出现问题请参考[kitex](https://www.cloudwego.io/zh/docs/kitex/getting-started/)

接下来安装ClyMQ：
1. 拉取ClyMQ
```
git@github.com:yclchuxue/ClyMQ.git
```
2. 使用kitex生成RPC代码：
```
cd ClyMQ

kitex -module ClyMQ -service ClyMQ operations.thrift

kitex -module ClyMQ -service ClyMQ raftoperations.thrift
```
接下来你就可以在你的代码中使用ClyMQ。
#### server
将代码中引入ClyMQ后，可选择以下操作来运行Server
1. 方式1
    ```
    sh build.sh
    ```
    执行上述命令后应该有一个output目录，其中包括编译产品。
    ```
    sh output/bootstrap.sh
    ```
    执行上述命令后Server开始运行
2. 方式2
    ``` 
    go run main.go
    ```
详细使用方法请参考[ZKServer使用文档]()和[Broker使用文档]()

#### producer
详细使用方法请参考[producer使用文档](https://github.com/yclchuxue/ClyMQ/blob/master/docs/producer%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3.md)

### consumer
详细使用方法请参考[consumer使用文档](https://github.com/yclchuxue/ClyMQ/blob/master/docs/consumer%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3.md)

### 常见问题

1. 若kitex生成的代码报错，则将go.mod中的github.com/apache/thrift v0.13.0，该成13版本

2. 构建 go.mod 文件
    ```
    go mod init 项目名
    ```
3. 加载 modules 模块
    ```
    go mod tidy
    ```