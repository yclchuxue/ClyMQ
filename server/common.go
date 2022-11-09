package server

import (
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
