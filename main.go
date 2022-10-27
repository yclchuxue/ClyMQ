package main

import (
	api "ClyMQ/kitex_gen/api/operations"
	"log"
)

func main() {
	svr := api.NewServer(new(OperationsImpl))

	err := svr.Run()

	if err != nil {
		log.Println(err.Error())
	}
}
