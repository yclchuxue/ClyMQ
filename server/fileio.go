package server

import (
	"encoding/json"
	"os"
	"sync"
)

type File struct{
	mu 			sync.RWMutex
	filename 	string
	node_size	int
}

func NewFile(name string) *File {
	return &File{
		mu: sync.RWMutex{},
		filename: name,
		node_size: 0,
	}
}

func (f *File) OpenFile() *os.File {
	f.mu.RLock()
	file, _ := CreateFile(f.filename)
	f.mu.RUnlock()

	return file
}

func(f *File) GetIndex(file *os.File) int64 {
	f.mu.RLock()
	var index int64

	/*
	读取文件，获取该partition的最后一个index
	*/

	f.mu.RUnlock()

	return index
}

func (f *File) WriteFile(file *os.File, node Key, msg []Message) bool {
	data_msg, err := json.Marshal(msg)
	if err != nil {
		DEBUG(dError, "%v turn json fail\n", msg)
	}
	node.Size = len(data_msg)
	data_node, err := json.Marshal(node)
	if err != nil{
		DEBUG(dError, "%v turn json fail\n", node)
	}

	f.mu.Lock()

	if f.node_size == 0 {
		f.node_size = len(data_node)
	}
	file.Write(data_node)
	file.Write(data_msg)

	f.mu.Unlock()

	if err != nil {
		return false
	}else{
		return true
	}
}

func (f *File) ReadFile(file *os.File) {
	
}