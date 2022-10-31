type Server struct {
	topics map[string]*Topic

	consumers map[string]*Client

	mu sync.Mutex
}

type Client struct{
	mu sync.RWMutex
	name string
	consumer client_operations.Client
	subList map[string]*SubScription  // 若这个consumer关闭则遍历这些订阅并修改
	// ingroups []*Group
	state string
}

type Group struct{
	rmu sync.RWMutex
	topic_name string
	consumers map[string]bool // map[client'name]alive
}

type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition
	subList map[string]*SubScription
}
type Partition struct {
	rmu              sync.RWMutex
	key              string
	queue            []string
	consumers 		 map[string]*Client
	consumers_offset map[string]int
}
type SubScription struct {
	rmu              sync.RWMutex
	name 			 string
	topic_name       string
	option           int8
	consumer_to_part map[string]string //consumer to partition
	groups           []*Group
	consistent       *Consistent
}

type Consistent struct {
	// 排序的hash虚拟节点（环形）
	hashSortedNodes []uint32
	// 虚拟节点(consumer)对应的实际节点
	circle 			map[uint32]string
	// 已绑定的consumer为true
	nodes  			map[string]bool

	mu sync.RWMutex
	//虚拟节点个数
	vertualNodeCount int
}