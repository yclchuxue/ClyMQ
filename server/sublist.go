package server

import "sync"

type Topic struct{
	Parts map[string]*Partition

}

type Partition struct{
	sync.RWMutex
	key string
	queue []Message
}

type Message struct{
	buf []byte
}