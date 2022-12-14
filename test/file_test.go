package main

import (
	"ClyMQ/server"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestFile1(t *testing.T) {
	fmt.Println("Test: File WriteFile and FindOffset and ReadFile")
	// server.Name = "Broker"
	topic_name := "phone_number"
	part_name := "xian"
	filename := "NowBlock.txt"
	messages := []string{"18700619719", "1234567891",
		"12345678911", "12345678912",
		"12345678913", "12345678914",
		"12345678915", "12345678916",
		"12345678917", "12345678918",
		"12345678919"}

	//删除文件夹下所有文件
	str, _ := os.Getwd()
	DeleteAllFiles(str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/", t)

	path := str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + filename
	file, fd, Err, err := server.NewFile(path)

	if err != nil {
		t.Fatal(Err, err.Error())
	}
	index := int64(11)
	var msgs []server.Message
	node := server.Key{
		Start_index: index,
	}

	for _, msg := range messages {
		msgs = append(msgs, server.Message{
			Index: index,
			Size:  1,
			Topic_name: topic_name,
			Part_name: part_name,
			Msg: []byte(msg),
		})
		index++
	}

	data_msg, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err.Error())
	}
	node.End_index = index-1
	node.Size = int64(len(data_msg))

	fmt.Println("---WriteFile node and data_msg")
	file.WriteFile(fd, node, data_msg)

	offset, err := file.FindOffset(fd, int64(13))
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("---FindOffset index 11 offset ", offset)

	_, MSGS, err := file.ReadFile(fd, offset)
	for index, m := range MSGS {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}	
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("   ... Passed")
}

func TestFile2(t *testing.T) {
	fmt.Println("Test: File WriteFile and FindOffset and ReadBytes")
	// server.Name = "Broker"
	topic_name := "phone_number"
	part_name := "xian"
	filename := "NowBlock.txt"
	messages := []string{"18700619719", "1234567891",
		"12345678911", "12345678912",
		"12345678913", "12345678914",
		"12345678915", "12345678916",
		"12345678917", "12345678918",
		"12345678919"}
	var MSGS []server.Message
	//删除文件夹下所有文件
	str, _ := os.Getwd()
	DeleteAllFiles(str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/", t)

	path := str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + filename
	file, fd, Err, err := server.NewFile(path)

	if err != nil {
		t.Fatal(Err, err.Error())
	}
	index := int64(11)
	var msgs []server.Message
	node := server.Key{
		Start_index: index,
	}

	for _, msg := range messages {
		msgs = append(msgs, server.Message{
			Index: index,
			Size:  1,
			Topic_name: topic_name,
			Part_name: part_name,
			Msg: []byte(msg),
		})
		index++
	}

	data_msg, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err.Error())
	}
	node.End_index = index-1
	node.Size = int64(len(data_msg))

	fmt.Println("---WriteFile node and data_msg")
	file.WriteFile(fd, node, data_msg)

	offset, err := file.FindOffset(fd, int64(13))
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("---FindOffset index 11 offset ", offset)

	_, data_MSGS, err := file.ReadBytes(fd, offset)

	json.Unmarshal(data_MSGS, &MSGS)

	for index, m := range MSGS {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}	
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("   ... Passed")
}

func TestFile3(t *testing.T) {
	fmt.Println("Test: File GetFirstIndex and GetIndex")
	// server.Name = "Broker"
	topic_name := "phone_number"
	part_name := "xian"
	filename := "NowBlock.txt"
	messages := []string{"18700619719", "1234567891",
		"12345678911", "12345678912",
		"12345678913", "12345678914",
		"12345678915", "12345678916",
		"12345678917", "12345678918",
		"12345678919"}
	//删除文件夹下所有文件
	str, _ := os.Getwd()
	DeleteAllFiles(str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/", t)

	path := str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + filename
	file, fd, Err, err := server.NewFile(path)

	if err != nil {
		t.Fatal(Err, err.Error())
	}
	index := int64(11)
	var msgs []server.Message
	node := server.Key{
		Start_index: index,
	}

	for _, msg := range messages {
		msgs = append(msgs, server.Message{
			Index: index,
			Size:  1,
			Topic_name: topic_name,
			Part_name: part_name,
			Msg: []byte(msg),
		})
		index++
	}

	data_msg, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err.Error())
	}
	node.End_index = index-1
	node.Size = int64(len(data_msg))

	fmt.Println("---WriteFile node and data_msg")
	file.WriteFile(fd, node, data_msg)

	first_index := file.GetFirstIndex(fd)
	if first_index != 11 {
		t.Fatal("   --- the first_index ", first_index, " != 11")
	}

	end_index := file.GetIndex(fd)
	if end_index != node.End_index {
		t.Fatal("   --- the end_index ", end_index, " != ", node.End_index)
	}

	fmt.Println("   ... Passed")
}