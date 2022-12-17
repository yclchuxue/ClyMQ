package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/logger"
	"net"
	"os"
	"runtime"
)

type PartKey struct {
	Name string `json:"name"`
}

//初始化Broker时的信息
type Options struct {
	Me 				   int
	Name               string
	Tag                string
	Zkserver_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
}

//Broker向ZKServer发送自己的新能指标,用于按权值负载均衡
type Property struct {
	Name    string `json:"name"`
	Power   int64  `json:"power"`
	CPURate int64  `json:"cpurate"`
	DiskIO  int64  `json:"diskio"`
}

//Broker启动时获得的初始信息，
type BroNodeInfo struct {
	Topics map[string]TopNodeInfo `json:"topics"`
}

type TopNodeInfo struct {
	Topic_name string
	Part_nums  int
	Partitions map[string]ParNodeInfo
}

type ParNodeInfo struct {
	Part_name  string
	Block_nums int
	Blocks     map[string]BloNodeInfo
}

type BloNodeInfo struct {
	Start_index int64
	End_index   int64
	Path        string
	File_name   string
}

type BrokerS struct {
	BroBrokers map[string]string `json:"brobrokers"`
	RafBrokers map[string]string `json:"rafbrokers"`
	Me_Brokers map[string]int    `json:"mebrokers"`
}

const (
	ZKBROKER = "zkbroker"
	BROKER   = "broker"

	// PTP_PUSH = 1
	// PTP_PULL = 2
	// PSB_PUSH = 3
	// PSB_PULL = 4
)

func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got: " + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr //获取本机MAC地址
		ipport += mac.String()
	}
	return ipport
}

func GetBlockName(file_name string) (ret string) {
	ret = file_name[:len(file_name)-4]
	return ret
}

func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func MovName(OldFilePath, NewFilePath string) error {
	return os.Rename(OldFilePath, NewFilePath)
}

func CreateList(path string) error {

	ret := CheckFileOrList(path)

	if !ret {
		err := os.Mkdir(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			logger.DEBUG(logger.DError, "%v:%v mkdir %v error %v\n", file, line, path, err.Error())
		}
	}

	return nil
}

func CreateFile(path string) (file *os.File, err error) {
	file, err = os.Create(path)

	return file, err
}

// func GetClisArray(clis map[string]*client_operations.Client) []string {
// 	var array []string

// 	for cli_name := range clis {
// 		array = append(array, cli_name)
// 	}

// 	return array
// }

func GetPartKeyArray(parts map[string]*Partition) []PartKey {
	var array []PartKey

	for part_name := range parts {
		array = append(array, PartKey{
			Name: part_name,
		})
	}

	return array
}

func CheckChangeCli(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for _, new_cli := range new {
		if _, ok := old[new_cli]; !ok { //new_cli 在old中没有
			add = append(add, new_cli)
		}
	}

	for old_cli := range old {
		had := false //不存在
		for _, name := range new {
			if old_cli == name {
				had = true
				break
			}
		}
		if !had {
			reduce = append(reduce, old_cli)
		}
	}

	return reduce, add
}


//use for Test
type Info struct {
	// name       string //broker name
	Topic_name string
	Part_name  string
	File_name  string
	New_name   string
	Option     int8
	Offset     int64
	Size       int8

	Ack int8

	Producer string
	Consumer string
	Cmdindex int64
	// startIndex  int64
	// endIndex  	int64
	Message []byte

	//raft
	Brokers map[string]string
	Me      int

	//fetch
	LeaderBroker string
	HostPort     string
}

//use in test
func GetInfo(in Info) info {
	return info{
		topic_name:   in.Topic_name,
		part_name:    in.Part_name,
		file_name:    in.File_name,
		new_name:     in.New_name,
		option:       in.Option,
		offset:       in.Offset,
		size:         in.Size,
		ack:          in.Ack,
		producer:     in.Producer,
		consumer:     in.Consumer,
		cmdindex:     in.Cmdindex,
		message:      in.Message,
		brokers:      in.Brokers,
		me:           in.Me,
		LeaderBroker: in.LeaderBroker,
		HostPort:     in.HostPort,
	}
}

func GetServerInfoAply() (chan info) {
	return make(chan info)
}
