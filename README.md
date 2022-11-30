### 使用说明
#### server
```
kitex -module ClyMQ -service ClyMQ operations.thrift

kitex -module ClyMQ -service ClyMQ raftoperations.thrift
```
执行上述命令，生成kitex RPC

若kitex生成的代码报错，则将go.mod中的github.com/apache/thrift v0.13.0，该成13版本

```
sh build.sh
```
执行上述命令后应该有一个output目录，其中包括编译产品。
```
sh output/bootstrap.sh
```
执行上述命令后Server开始运行
#### client
```
go run client/main.go
```

### 常见问题

构建 go.mod 文件
```
go mod init 项目名
```
加载 modules 模块
```
go mod tidy
```