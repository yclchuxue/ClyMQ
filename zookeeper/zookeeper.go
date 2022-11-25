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

	Root       string
	BrokerRoot string
	TopicRoot  string
}

type ZkInfo struct {
	HostPorts []string
	Timeout   int
	Root      string
}

//root = "/ClyMQ"
func NewZK(info ZkInfo) *ZK {
	coon, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		fmt.Println(err.Error())
	}
	return &ZK{
		conn:       coon,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}
}

type BrokerNode struct {
	Name     string `json:"name"`
	HostPort string `json:"hostPort"`
	Pnum     int    `json:"pNum"`
	//一些负载情况
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pNum"`
	// Brokers []string `json:"brokers"` //保存该topic的partition现在有那些broker负责，
	//用于PTP的情况
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	Option    int8   `json:"option"`    //partition的状态
	DupNum    int8   `json:"dupNum"`
	PTPoffset int64  `json:"ptpOffset"`
}

type BlockNode struct {
	Name          string `json:"name"`
	FileName      string `json:"filename"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`

	LeaderBroker string `json:"leaderBroker"`
}

type DuplicateNode struct {
	StartOffset int64  `json:"startOffset"`
	EndOffset   int64  `json:"endOffset"`
	BrokerName  string `json:"brokerName"`
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

	ok, _, err := z.conn.Exists(path)
	if ok {
		return err
	}

	_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) GetPartState(topic_name, part_name string) PartitionNode {
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	data, _, _ := z.conn.Get(path)
	var node PartitionNode
	json.Unmarshal(data, &node)

	return node
}

func (z *ZK) CreateState(name string) error {
	path := z.BrokerRoot + "/" + name + "/state"
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	_, err = z.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

type Part struct {
	Topic_name string
	Part_name  string
	BrokerName string
	Host_Port  string
	PTP_index  int64
	File_name  string
	Err        string
}

//检查broker是否在线
func (z *ZK) CheckBroker(broker_path string) bool {
	ok, _, _ := z.conn.Exists(broker_path + "/state")
	if ok {
		return true
	} else {
		return false
	}
}

//consumer 获取PTP的Brokers //（和PTP的offset）
func (z *ZK) GetBrokers(topic string) ([]Part, error) {
	path := z.TopicRoot + "/" + topic + "/" + "partitions"
	// var tnode TopicNode
	ok, _, err := z.conn.Exists(path)

	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	var Parts []Part

	partitions, _, _ := z.conn.Children(path)
	for _, part := range partitions {

		PTP_index := z.GetPartitionPTPIndex(path + "/" + part)

		var max_dup DuplicateNode
		max_dup.EndOffset = 0
		blocks, _, _ := z.conn.Children(path + "/" + part)
		for _, block := range blocks {
			info := z.GetBlockNode(path + "/" + part + "/" + block)
			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {

				Duplicates, _, _ := z.conn.Children(path + "/" + part + "/" + info.Name)
				for _, duplicate := range Duplicates {

					duplicatenode := z.GetDuplicateNode(path + "/" + part + "/" + info.Name + "/" + duplicate)
					if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
						//保证broker在线
						if z.CheckBroker(duplicatenode.BrokerName) {
							max_dup = duplicatenode
						}
					}
				}
				var ret string
				if max_dup.EndOffset != 0 {
					ret = "OK"
				} else {
					ret = "thr brokers not online"
				}
				//一个partition只取endoffset最大的broker,其他小的broker副本不全面
				broker := z.GetBrokerNode(max_dup.BrokerName)
				Parts = append(Parts, Part{
					Topic_name: topic,
					Part_name:  part,
					BrokerName: broker.Name,
					Host_Port:  broker.HostPort,
					PTP_index:  PTP_index,
					File_name:  info.FileName,
					Err:        ret,
				})
				break
			}
		}
	}

	return Parts, nil
}

func (z *ZK) GetBroker(topic, part string, offset int64) (parts []Part, err error) {
	part_path := z.TopicRoot + "/" + topic + "/partitions/" + part
	ok, _, err := z.conn.Exists(part_path)
	if !ok || err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	var max_dup DuplicateNode
	max_dup.EndOffset = 0
	blocks, _, _ := z.conn.Children(part_path)
	for _, block := range blocks {
		info := z.GetBlockNode(part_path + "/" + block)

		if info.StartOffset <= offset && info.EndOffset >= offset {

			Duplicates, _, _ := z.conn.Children(part_path + "/" + block)
			for _, duplicate := range Duplicates {

				duplicatenode := z.GetDuplicateNode(part_path + "/" + block + "/" + duplicate)
				if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
					//保证broker在线
					if z.CheckBroker(duplicatenode.BrokerName) {
						max_dup = duplicatenode
					}
				}
			}
			var ret string
			if max_dup.EndOffset != 0 {
				ret = "OK"
			} else {
				ret = "thr brokers not online"
			}
			//一个partition只取endoffset最大的broker,其他小的broker副本不全面
			broker := z.GetBrokerNode(max_dup.BrokerName)
			parts = append(parts, Part{
				Topic_name: topic,
				Part_name:  part,
				BrokerName: broker.Name,
				Host_Port:  broker.HostPort,
				File_name:  info.FileName,
				Err:        ret,
			})
			break
		}
	}
	return parts, nil
}

type StartGetInfo struct {
	Cli_name      string
	Topic_name    string
	PartitionName string
	Option        int8
}

func (z *ZK) CheckSub(info StartGetInfo) bool {

	//检查该consumer是否订阅了该topic或partition

	return true
}

func (z *ZK) GetPartNowBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode) {
	now_block_path := z.TopicRoot + "/" + topic_name + "/" + "partitions" + "/" + part_name + "/" + "NowBlock"
	for {
		NowBlock := z.GetBlockNode(now_block_path)
		Broker := z.GetBrokerNode(NowBlock.LeaderBroker)
		ret := z.CheckBroker(z.BrokerRoot + "/" + Broker.Name)
		if ret {
			return Broker, NowBlock
		} else {
			time.Sleep(time.Second * 1)
		}
	}
}

func (z *ZK) GetBlockSize(topic_name, part_name string) (int, error) {
	path := z.TopicRoot + "/" + topic_name + "/partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return 0, err
	}

	parts, _, err := z.conn.Children(path)
	if err != nil {
		return 0, err
	}
	return len(parts), nil
}

func (z *ZK) GetBrokerNode(name string) BrokerNode {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &bronode)

	return bronode
}

func (z *ZK) GetPartitionPTPIndex(path string) int64 {
	var pnode PartitionNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &pnode)

	return pnode.PTPoffset
}

func (z *ZK) GetBlockNode(path string) BlockNode {
	var blocknode BlockNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &blocknode)

	return blocknode
}

func (z *ZK) GetDuplicateNodes(topic_name, part_name, block_name string) (nodes []DuplicateNode) {
	BlockPath := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name + "/" + block_name
	Dups, _, _ := z.conn.Children(BlockPath)

	for _, dup_name := range Dups {
		DupNode := z.GetDuplicateNode(BlockPath + "/" + dup_name)
		nodes = append(nodes, DupNode)
	}

	return nodes
}

func (z *ZK) GetDuplicateNode(path string) DuplicateNode {
	var dupnode DuplicateNode
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &dupnode)

	return dupnode
}

//获取当前partition接收信息的文件的文件名
//返回需要修改成为的文件名


//将partition的NowBlock的信息中的文件名修改
//并创建一个新Block节点，将原来Nowlock节点的内容存到这个节点
//并将NowBlock中的节点信息更新；
