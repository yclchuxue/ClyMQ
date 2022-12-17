package server

import (
	"ClyMQ/logger"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type File struct {
	mu        sync.RWMutex
	filename  string
	node_size int
}

//后期加入sstable加速查找offset
// type SStable struct {
// 	table  	[64][2]int
// }

//先检查该磁盘是否存在该文件，如不存在则需要创建
func NewFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {
		fd, err = CreateFile(path_name)
		if err != nil {
			Err = "CreatFileFail"
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return nil, nil, Err, err
		} 
		// else {
		// 	logger.DEBUG(logger.DLog, "not find file(%v), create a file\n", path_name)
		// }
	} else {
		fd, err = os.OpenFile(path_name, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "OpenFile"
			return nil, nil, Err, err
		} 
		// else {
		// 	logger.DEBUG(logger.DLog, "find file(%v), open this file\n", path_name)
		// }
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
}

func CheckFile(path_name string) (file *File, fd *os.File, Err string, err error) {
	if !CheckFileOrList(path_name) {
		Err = "NotFile"
		err = errors.New(Err)
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, nil, Err, err
	} else {
		fd, err = os.OpenFile(path_name, os.O_RDONLY, 0666)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			Err = "OpenFile"
			return nil, nil, Err, err
		}
	}

	file = &File{
		mu:        sync.RWMutex{},
		filename:  path_name,
		node_size: NODE_SIZE,
	}

	logger.DEBUG(logger.DLog, "the file is %v\n", file)

	return file, fd, "ok", err
}

//修改文件名
func (f *File) Update(path, file_name string) error {
	OldFilePath := f.filename
	NewFilePath := path + "/" + file_name
	// logger.DEBUG(logger.DdLog, "path is %v in.filename is %v newname is %v\n", path, f.filename, file_name)
	// logger.DEBUG(logger.DdLog, "oldname is %v newname is %v\n", OldFilePath, NewFilePath)
	f.mu.Lock()
	f.filename = NewFilePath
	f.mu.Unlock()

	return MovName(OldFilePath, NewFilePath)
}

func (f *File) OpenFileRead() *os.File {
	// logger.DEBUG(logger.DLog, "the file name is %v\n", f.filename)
	f.mu.RLock()
	fd, err := os.OpenFile(f.filename, os.O_RDONLY, 0666)
	f.mu.RUnlock()
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil
	}	

	return fd
}

func (f *File) OpenFileWrite() *os.File {
	f.mu.RLock()
	fd, err := os.OpenFile(f.filename, os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
	f.mu.RUnlock()
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil
	}	

	return fd
}

func (f *File) GetFirstIndex(file *os.File) int64 {
	var node Key
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	_, err := file.ReadAt(data_node, 0)

	if err == io.EOF {
		//读到文件末尾
		logger.DEBUG(logger.DLeader, "read All file, the first_index is %v\n", 0)
		return 0
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)

	// logger.DEBUG(logger.DLog, "the node is %v\n", node)
	return node.Start_index
}

//读取文件，获取该partition的最后一个index
func (f *File) GetIndex(file *os.File) int64 {
	var node Key
	var index, offset int64
	index = -1
	offset = 0
	data_node := make([]byte, NODE_SIZE)
	f.mu.RLock()
	// 读取文件，获取该partition的最后一个index
	defer f.mu.RUnlock()

	for {
		_, err := file.ReadAt(data_node, offset)

		if err == io.EOF {
			//读到文件末尾
			// logger.DEBUG(logger.DLeader, "read All file, get end_index is %v\n", node.End_index)
			if index == 0 {
				index = node.End_index
			} else {
				index = 0
			}
			return index
		} else {
			index = 0
		}
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, data_node)
		binary.Read(buf, binary.BigEndian, &node)

		offset += offset + NODE_SIZE + node.Size
	}
}

func (f *File) WriteFile(file *os.File, node Key, data_msg []byte) bool {
	data_node := &bytes.Buffer{}
	err := binary.Write(data_node, binary.BigEndian, node)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		logger.DEBUG(logger.DError, "%v turn bytes fail\n", node)
		return false
	}

	// logger.DEBUG(logger.DdLog, "the node size is %v\n", len(data_node.Bytes()))
	f.mu.Lock()

	if f.node_size == 0 {
		f.node_size = len(data_node.Bytes())
	}
	file.Write(data_node.Bytes())
	file.Write(data_msg)

	f.mu.Unlock()

	if err != nil {
		return false
	} else {
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
	// logger.DEBUG(logger.DdLog, "the size is %v, node is %v\n", size, data_node)
	if size != NODE_SIZE {
		return node, msg, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {
		//读到文件末尾
		logger.DEBUG(logger.DLeader, "read All file, do not find this index")
		return node, msg, err
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	data_msg := make([]byte, node.Size)
	offset += int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	// logger.DEBUG(logger.DdLog, "the size is %v, data is %v offset is %v, node.Size is %v\n", size, data_node, offset, node.Size)
	if int64(size) != node.Size {
		return node, msg, errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾
		return node, msg, errors.New("read All file, do not find this index")
	}

	json.Unmarshal(data_msg, &msg)

	return node, msg, nil
}

func (f *File) ReadBytes(file *os.File, offset int64) (Key, []byte, error) {
	var node Key
	data_node := make([]byte, NODE_SIZE)

	f.mu.RLock()
	defer f.mu.RUnlock()

	size, err := file.ReadAt(data_node, offset)

	if size != NODE_SIZE {
		return node, nil, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾.
		logger.DEBUG(logger.DLeader, "read All file, do not find this index")
		return node, nil, err
	}

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	data_msg := make([]byte, node.Size)
	offset += +int64(f.node_size)
	size, err = file.ReadAt(data_msg, offset)

	if int64(size) != node.Size {
		return node, nil, errors.New("read msg size is not NODE_SIZE")
	}
	if err == io.EOF { //读到文件末尾
		return node, nil, errors.New("read All file, do not find this index")
	}

	return node, data_msg, nil
}

func (f *File) FindOffset(file *os.File, index int64) (int64, error) {
	var node Key

	data_node := make([]byte, NODE_SIZE)

	offset := int64(0)
	for {
		logger.DEBUG(logger.DLog, "the file name is %v\n", f.filename)
		f.mu.RLock()
		size, err := file.ReadAt(data_node, offset)
		f.mu.RUnlock()

		if err == io.EOF { //读到文件末尾
			return index, errors.New("read All file, do not find this index")
		}
		if size != NODE_SIZE {
			logger.DEBUG(logger.DLog2, "the size is %v NODE_SIZE is %v\n", size, NODE_SIZE)
			return int64(-1), errors.New("read node size is not NODE_SIZE")
		}
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, data_node)
		binary.Read(buf, binary.BigEndian, &node)
		// logger.DEBUG(logger.DLog2, "the node is %v size is %v\n", node, size)
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size)
		} else {
			break
		}
	}

	return offset, nil
}
