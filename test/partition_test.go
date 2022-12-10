package main

import (
	"ClyMQ/server"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func DeleteAllFiles(path string, t *testing.T) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		t.Fatal(err.Error())
	}
    for _, d := range dir {
        err = os.RemoveAll(path+d.Name())
		if err != nil {
			t.Fatal(err.Error())
		}
    }
}

func TestPartitionAccept1(t *testing.T) {
	fmt.Println("Test: Partition accept message use addMessage")
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
	str, _ := os.Getwd()
	DeleteAllFiles(str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/", t)

	Partition := server.NewPartition("Broker", topic_name, part_name)
	path := str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + filename
	file, fd, Err, err := server.NewFile(path)

	//清空文件
	// fd.Truncate(0)
	// fd.Seek(0, 0)

	if err != nil {
		t.Fatal(Err, err.Error())
	}

	ret := Partition.StartGetMessage(file, fd, server.GetInfo(server.Info{
		Topic_name: topic_name,
		Part_name:  part_name,
		File_name:  filename,
	}))
	fmt.Println("---the StartGetMessage return ", ret)

	for _, msg := range messages {
		ret, err = Partition.AddMessage(server.GetInfo(server.Info{
			Producer:   "TestPartitionAccept1",
			Topic_name: topic_name,
			Part_name:  part_name,
			Message:    []byte(msg),
			Size:       0,
		}))
		// fmt.Println( "queue is ", Partition.GetQueue())
		if err != nil {
			t.Fatal(ret, err.Error())
		}
	}

	_, msgs, err := file.ReadFile(fd, 0)
	// fmt.Println(key, msgs)
	for index, m := range msgs {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}	
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("   ... Passed")
}


func TestPartitionCloseAccept(t *testing.T) {
	fmt.Println("Test: Partition accept message use addMessage")
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

	str, _ := os.Getwd()
	DeleteAllFiles(str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/", t)


	Partition := server.NewPartition("Broker", topic_name, part_name)
	path := str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + filename
	file, fd, Err, err := server.NewFile(path)

	//清空文件
	// fd.Truncate(0)
	// fd.Seek(0, 0)

	if err != nil {
		t.Fatal(Err, err.Error())
	}

	ret := Partition.StartGetMessage(file, fd, server.GetInfo(server.Info{
		Topic_name: topic_name,
		Part_name:  part_name,
		File_name:  filename,
	}))
	fmt.Println("---the StartGetMessage return ", ret)

	for _, msg := range messages {
		ret, err = Partition.AddMessage(server.GetInfo(server.Info{
			Producer:   "TestPartitionAccept1",
			Topic_name: topic_name,
			Part_name:  part_name,
			Message:    []byte(msg),
			Size:       0,
		}))
		// fmt.Println( "queue is ", Partition.GetQueue())
		if err != nil {
			t.Fatal(ret, err.Error())
		}
	}

	start, end, ret, err := Partition.CloseAcceptMessage(server.GetInfo(server.Info{
		Topic_name: topic_name,
		Part_name: part_name,
		File_name: filename,
		New_name:  "Block1.txt",
	}))

	if err != nil {
		t.Fatal("the file start", start, " end ", end, " ret ", ret, " err is ", err.Error())
	}
	fmt.Println("the file start", start, " end ", end, " ret ", ret)

	fd.Close()


	path = str + "/" + "Broker" + "/" + topic_name + "/" + part_name + "/" + "Block1.txt"
	file, fd, Err, err = server.NewFile(path)
	if err != nil {
		t.Fatal("the Err is ", Err, "error ", err.Error())
	}

	_, msgs, err := file.ReadFile(fd, 0)
	// fmt.Println(key, msgs)
	for index, m := range msgs {
		if string(m.Msg) != messages[index] {
			t.Fatal("---the reading != writing")
		}	
	}
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("   ... Passed")
}