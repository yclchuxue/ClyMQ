### 使用说明
#### server
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