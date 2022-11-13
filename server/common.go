package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	"fmt"
	"net"
	"os"
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

func CheckFileOrList(filename string) (ret bool) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		ret = false
	}
	ret = true
	return ret
}

func CreateList(path string) error {
	err := os.Mkdir(path, 0666)
	if err != nil {
		fmt.Println("mkdir ", path, "error")
	}

	return err
}

func CreateFile(path string) (file *os.File, err error) {
	file, err = os.Create(path);

	return file, err
}

func GetClisArray(clis map[string]*client_operations.Client) []string {
	var array []string

	for cli_name := range clis {
		array = append(array, cli_name)
	}

	return array
}

func CheckChangeCli(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for _, new_cli := range new {
		if _, ok := old[new_cli]; !ok {  //new_cli 在old中没有
			add = append(add, new_cli)
		}
	}

	for old_cli := range old {
		had := false   //不存在
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