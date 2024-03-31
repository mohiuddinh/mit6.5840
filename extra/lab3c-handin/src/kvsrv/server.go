package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	data map[string]string
	clientTable map[int64]map[int]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock() 
	defer kv.mu.Unlock() 

	if value, exists := kv.data[args.Key]; exists {
		reply.Value = value 
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock() 
	defer kv.mu.Unlock() 

	// if val, ok := kv.ExistsInClientTable(args.ClientID, args.UUID); ok {
	// 	reply.Value = val 
	// 	return 
	// }

	kv.data[args.Key] = args.Value 
	reply.Value = "" 
	// kv.AddEntryToClientTable(args.ClientID, args.UUID, reply.Value)
}

func (kv *KVServer) ExistsInClientTable(clientID int64, uuid int) (response string, ok bool) {
	if subMap, ok := kv.clientTable[clientID]; ok {
		if value, exists := subMap[uuid]; exists {
			return value, true 
		}
	}
	return "", false 
}

func (kv *KVServer) AddEntryToClientTable(clientID int64, uuid int, value string) {
	subMap := map[int]string{uuid: value}
	kv.clientTable[clientID] = subMap
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock() 
	defer kv.mu.Unlock() 

	if val, ok := kv.ExistsInClientTable(args.ClientID, args.UUID); ok {
		reply.Value = val 
		return  
	}

	if value, exists := kv.data[args.Key]; exists {
		reply.Value = value 
		kv.data[args.Key] = value + args.Value 
	} else {
		reply.Value = "" 
		kv.data[args.Key] = args.Value
	}
	kv.AddEntryToClientTable(args.ClientID, args.UUID, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.clientTable = make(map[int64]map[int]string)
	return kv
}
