type Server struct {
	topics map[string]*Topic

	consumers map[string]*Client

	mu sync.Mutex
}

type Client struct{
	mu sync.Mutex
	name string
	consumer client_operations.Client

	state string
}

type Group struct{
	topics *Topic
	consumers_alive map[string]*Client   //partitionname -> client
	consumers_down  map[string]*Client
}

type Topic struct{
	sync.RWMutex
	Parts map[string]*Partition
	SubScription_Key_PSB map[string]*SubScription
	SubScription_NIL_PTP *SubScription
}

type Partition struct{
	sync.RWMutex
	key string
	queue []string
	consumers_offset map[string]int
}

type SubScription struct{
	topic *string
	option int8
	parts map[string]*Partition   //key to partition
	groups []*Group
}