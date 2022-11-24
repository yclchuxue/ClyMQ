package server

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type File struct{
	mu 			sync.RWMutex
	filename 	string
	node_size	int
}

//先检查该磁盘是否存在该文件，如不存在则需要创建
func NewFile(path_name string) (file *File, fd *os.File) {
	var err error
	if !CheckFileOrList(path_name) {
		fd, err = CreateFile(path_name)
		if err != nil {
			DEBUG(dError, err.Error())
		}
	}else{
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			DEBUG(dError, err.Error())
		}
	}

	file = &File{
		mu: sync.RWMutex{},
		filename: name,
		node_size: NODE_SIZE,
	}
	return file, fd
}

//修改文件名
func (f *File)Update(file_name string) {

} 

func (f *File) OpenFile() *os.File {
	f.mu.RLock()
	file, _ := CreateFile(f.filename)
	f.mu.RUnlock()

	return file
}

func(f *File) GetIndex(file *os.File) int64 {
	f.mu.RLock()
	// var index int64

	/*
	读取文件，获取该partition的最后一个index
	*/

	f.mu.RUnlock()

	return 0
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

func (f *File) ReadFile(file *os.File, offset int64) (Key, []Message, error) {
	var node Key
	var msg []Message
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()
	
	size, err := file.ReadAt(data_node, offset)

	if size != NODE_SIZE {
		return node, msg,  errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {    //读到文件末尾
		return node, msg, errors.New("read All file, do not find this index")
	}

	json.Unmarshal(data_node, &node)
	data_msg := make([]byte, node.Size)
	size, err = file.ReadAt(data_msg, offset + int64(f.node_size) + int64(node.Size) )

	if size != NODE_SIZE {
		return node, msg,  errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF {    //读到文件末尾
		return node, msg, errors.New("read All file, do not find this index")
	}

	json.Unmarshal(data_msg, &msg)

	return node, msg, nil
}

func (f *File) ReadByte(file *os.File, offset int64) (Key, []byte, error) {
	var node Key

	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()
	
	size, err := file.ReadAt(data_node, offset)

	if size != NODE_SIZE {
		return node, nil,  errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {    //读到文件末尾
		return node, nil, errors.New("read All file, do not find this index")
	}

	json.Unmarshal(data_node, &node)
	data_msg := make([]byte, node.Size)
	size, err = file.ReadAt(data_msg, offset + int64(f.node_size) + int64(node.Size) )

	if size != NODE_SIZE {
		return node, nil,  errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF {    //读到文件末尾
		return node, nil, errors.New("read All file, do not find this index")
	}

	return node, data_msg, nil
}

func (f *File) FindOffset(file *os.File, index int64) (int64, error){
	var node Key
	data_node := make([]byte, NODE_SIZE)

	offset := int64(0)
	for{

		f.mu.RLock()
		size, err := file.ReadAt(data_node, offset)
		f.mu.RUnlock()

		if size != NODE_SIZE {
			return int64(-1),  errors.New("read node size is not NODE_SIZE")
		}
		if err == io.EOF {    //读到文件末尾
			return index, errors.New("read All file, do not find this index")
		}

		json.Unmarshal(data_node, &node)
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size)
		}else{
			break
		}
	}

	return offset, nil
}