package zookeeper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZK struct {
	conn *zk.Conn

	Root string
	BrokerRoot string
	TopicRoot  string
}

type ZkInfo struct {
	HostPorts []string
	Timeout 	int
	Root 		string
}

//root = "/ClyMQ"
func NewZK(info ZkInfo) *ZK {
	coon, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		fmt.Println(err.Error())
	}
	return &ZK{
		conn: coon,
		Root: info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}
}

type BrokerNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
	Pnum int    `json:"pnum"`
	//一些负载情况
}

type TopicNode struct {
	Name    string   `json:"name"`
	Pnum    int      `json:"pnum"`
	Brokers []string `json:"brokers"` //保存该topic的partition现在有那些broker负责，
	//用于PTP的情况
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	PTPoffset int64	 `json:"ptpoffset"`
}

type BlockNode struct {
	Name        	string `json:"name"`
	TopicName 		string `json:"topicname"`
	PartitionName 	string `json:"partitionname"`
	StartOffset 	int64  `json:"startoffset"`
	EndOffset   	int64  `json:"endoffset"`
	BrokerName  	string `json:"brokername"`
}

func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path += z.BrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += z.TopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += z.TopicRoot + "/" + pnode.TopicName + "/" + pnode.Name
		data, err = json.Marshal(pnode)
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode)
	}

	if err != nil {
		return err
	}

	_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	return nil
}


//consumer 获取PTP的Brokers //（和PTP的offset）
func (z *ZK) GetBrokers(topic string) ([]string, error) {
	path := z.TopicRoot + "/" + topic
	var tnode TopicNode
	ok, _, err := z.conn.Exists(path)

	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	data, _, err := z.conn.Get(path)

	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	} 
	err = json.Unmarshal(data, &tnode)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return tnode.Brokers, nil
}

//consumer 和 producer 获取现在该消费位置和存放位置的Broker
// func (z *ZK) GetBroker(topic, partition, option string, offset int64) (string, error){
// 	path := z.TopicRoot + "/" + topic + "/" + partition

// 	ok, _, err := z.conn.Exists(path)
// 	if !ok || err != nil {
// 		fmt.Println(err.Error())
// 		return "", err
// 	}

	
// }