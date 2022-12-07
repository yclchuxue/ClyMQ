package main

import (
	"ClyMQ/server"
	"fmt"
	"strconv"
	"testing"
)

func CheckNums(PartToCon map[string][]string, min, max int, nums int) (ret string, b bool) {
	size := 0
	for key, value := range PartToCon {
		if len(value) < min {
			ret += "the consumer" + key + " responsible " + strconv.Itoa(len(value)) + " part < min(" + strconv.Itoa(min) + ")\n"
		}else if len(value) > max {
			ret += "the consumer" + key + " responsible " + strconv.Itoa(len(value)) + " part > max(" + strconv.Itoa(max) + ")\n"
		}
		size++
	}
	if size != nums {
		ret += "the consumers responsible parts quantity(" + strconv.Itoa(size) + ") != nums(" + strconv.Itoa(nums) + ")\n"
	}

	if ret != "" {
		return ret, false
	}
	return ret, true
}

func TestConsistent1(t *testing.T) {
	fmt.Println("Test: Consistent partitions > consumers")

	partitions := []string{"xian", "shanghai", "beijing"}
	consumers  := []string{"consumer1", "consumer2"}

	PartToCon := make(map[string][]string)

	fmt.Println("----node is consumer")	
	consistent := server.NewConsistent()

	for _, name := range consumers {
		consistent.Add(name, 1)
	}
	
	consistent.SetFreeNode()
	// fmt.Println("Free Node Now nums is ", consistent.GetFreeNodeNum())
	fmt.Println("----start first getnode")
	for _, name := range partitions {
		node := consistent.GetNode(name)
		PartToCon[name] = append(PartToCon[name], node)
	}

	fmt.Println("----start second getnode")
	for {
		for _, name := range partitions {
			// fmt.Println("Free Node Now nums is ", consistent.GetFreeNodeNum())
			if consistent.GetFreeNodeNum() > 0{
				node := consistent.GetNodeFree(name)
				PartToCon[name] = append(PartToCon[name], node)
			}else{
				break
			}
		}
		if consistent.GetFreeNodeNum() <= 0{
			break
		}
	}
	//检查partition 对应的 Consumer的数量是否平衡，和是否每个partition都有consumer负责
	ret, ok := CheckNums(PartToCon, 1, 2, len(partitions))
	if !ok {
		t.Fatal(ret, PartToCon)
	}

	fmt.Println("  ... Passed")
}

func TestConsistent2(t *testing.T) {
	fmt.Println("Test: Consistent partitions < consumers")

	partitions := []string{"xian", "shanghai", "beijing"}
	consumers  := []string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}

	// partitions := []string{"123", "124", "125"}
	// consumers  := []string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}

	PartToCon := make(map[string][]string)

	fmt.Println("----node is consumer")	
	consistent := server.NewConsistent()

	for _, name := range consumers {
		consistent.Add(name, 1)
	}
	consistent.SetFreeNode()
	// fmt.Println("Free Node Now nums is ", consistent.GetFreeNodeNum())
	fmt.Println("----start first getnode")
	for _, name := range partitions {
		node := consistent.GetNode(name)
		PartToCon[name] = append(PartToCon[name], node)
	}

	fmt.Println("----start second getnode")
	for {
		for _, name := range partitions {
			// fmt.Println("Free Node Now nums is ", consistent.GetFreeNodeNum())
			if consistent.GetFreeNodeNum() > 0{
				node := consistent.GetNodeFree(name)
				PartToCon[name] = append(PartToCon[name], node)
			}else{
				break
			}
		}
		if consistent.GetFreeNodeNum() <= 0{
			break
		}
	}

	//检查partition 对应的 Consumer的数量是否平衡，和是否每个partition都有consumer负责
	ret, ok := CheckNums(PartToCon, 1, 2, len(partitions))
	if !ok {
		t.Fatal(ret, PartToCon)
	}

	fmt.Println("  ... Passed")
}

func TestConsistentBro1(t *testing.T) {
	fmt.Println("Test: ConsistentBro ")

	dup_num := 3
	topic_name := "phone_number"
	partitions := []string{"xian", "shanghai", "beijing"}
	brokers  := []string{"Broker0", "Broker1", "Broker2"}

	// partitions := []string{"123", "124", "125"}
	// consumers  := []string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}

	// PartToBro := make(map[string][]string)

	fmt.Println("----node is brokers")
	consistent := server.NewConsistentBro()

	for _, name := range brokers {
		consistent.Add(name, 1)
	}

	dups := make(map[string]bool)
	str := topic_name + partitions[0]
	Bro_dups := consistent.GetNode(str, dup_num)
	// fmt.Println(Bro_dup_1)
	for _, name := range Bro_dups {
		dups[name] = true
	} 
	if len(dups) != dup_num{
		t.Fatal("get dup brokers ", len(dups), " != ", dup_num)
	}
	
	fmt.Println("  ... Passed")
}