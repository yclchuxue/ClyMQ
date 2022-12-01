package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

var (
    file *os.File
    err     error
)

type Message struct {
	Index 		int64       `json:“index"`
	Topic_name 	string      `json:"topic_name"`
	Part_name 	string      `json:"part_name"`
	Msg 		string      `json:"msg"`
}

type node  struct{
    Start_index int64   `json:"start_index"`
    End_index   int64   `json:"end_index"`
    Size        int64   `json:"size"`
}

//test file apis
func main() {

	str, _ := os.Getwd()
    file, err = os.Create(str+"/test1.txt")
    if err != nil {
        fmt.Println(err)
    }

    var msg []Message

    t1 := Message{
        Index: 1,
        Topic_name: "小说",
        Part_name: "玄幻",
        Msg: "abcdefg_1",
    }

    t2 := Message{
        Index: 1,
        Topic_name: "小说",
        Part_name: "玄幻",
        Msg: "abcdefg_2",
    }

    t3 := Message{
        Index: 1,
        Topic_name: "小说",
        Part_name: "玄幻",
        Msg: "abcdefg_3",
    }

    t4 := Message{
        Index: 1,
        Topic_name: "小说",
        Part_name: "玄幻",
        Msg: "abcdefg_4",
    }

    // fmt.Println(t1)
    
    msg = append(msg, t1)
    msg = append(msg, t2)
    msg = append(msg, t3)
    msg = append(msg, t4)

    data_msg, _ := json.Marshal(msg)

    no := node{
        Start_index: msg[0].Index,
        End_index: msg[len(msg)-1].Index,
        Size: int64(len(data_msg)),
    }

    buf := &bytes.Buffer{}

    err := binary.Write(buf, binary.BigEndian, no)
    if err != nil {
        panic(err)
    }

    var no_node node
    var msg_message []Message

    file.Write(buf.Bytes())
    file.Write(data_msg)

	

    no_data := make([]byte, len(buf.Bytes()))
    file.ReadAt(no_data, int64(0))

    fmt.Println("the node size is ", len(buf.Bytes()))

    
    
    buf1 := &bytes.Buffer{}
    binary.Write(buf1, binary.BigEndian, buf.Bytes())
    err = binary.Read(buf, binary.BigEndian, &no_node)
    if err != nil {
        panic(err)
    }

    fmt.Println(no_node)   //索引信息

    msg_data := make([]byte, no_node.Size)
    file.ReadAt(msg_data, int64(no_node.Size))

    
    json.Unmarshal(data_msg, &msg_message)
    fmt.Println(msg_message)


    file.Close()
	// str, _ := os.Getwd()	
	// path1 := str + "/" + "Broker" + "/" + "phone_number" + "/" + "xian" + "/" + "NowBlock.txt"
	// path2 := str + "/" + "Broker" + "/" + "phone_number" + "/" + "xian" + "/" + "Block1.txt"


}