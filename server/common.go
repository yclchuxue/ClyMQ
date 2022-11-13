package server

import (
	"ClyMQ/kitex_gen/api/client_operations"
	"net"
	"os"
	"runtime"
)

type PartKey struct{
	name string `json:"name"`
}

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

func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path)    //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func CreateList(path string) error {

	ret := CheckFileOrList(path)

	if !ret {
		err := os.Mkdir(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			DEBUG(dError, "%v:%v mkdir %v error %v",file, line, path, err.Error())
		}
	}

	return nil
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

func GetPartKeyArray(parts map[string]*Partition) []PartKey {
	var array []PartKey

	for part_name := range parts {
		array = append(array, PartKey{
			name: part_name,
		})
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