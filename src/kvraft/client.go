package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	clerkId int64
	commandId int64 
	mu sync.Mutex
	raftLeader int 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand() 
	ck.raftLeader = 0
	ck.commandId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock() 
	myCommandId := ck.commandId
	ck.commandId++
	pollServer := ck.raftLeader
	ck.mu.Unlock() 

	args := GetArgs{Key: key, CommandId: myCommandId, ClerkId: ck.clerkId}
	output := ""
	for {
		reply := GetReply{} 
		ok := ck.servers[pollServer].Call("KVServer.Get", &args, &reply)
		DPrint("In client with get and sent request ", args, " received okay with pollserver", pollServer, " and reply ", reply)
		if ok && (reply.Status == OK || reply.Status == ErrNoKey) {
			if reply.Status == OK {
				output = reply.Value
			}
			break
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
	
	ck.mu.Lock() 
	ck.raftLeader = pollServer
	ck.mu.Unlock()
	
	return output
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock() 
	myCommandId := ck.commandId
	ck.commandId++
	pollServer := ck.raftLeader
	ck.mu.Unlock() 

	args := PutAppendArgs{Key: key, Value: value, CommandId: myCommandId, ClerkId: ck.clerkId, Type: op}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[pollServer].Call("KVServer.PutAppend", &args, &reply)
		DPrint("In client with ", op, " and sent request ", args, " received okay with pollserver", pollServer, " and reply ", reply)
		if ok && reply.Status == OK {
			break 
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
	
	ck.mu.Lock() 
	ck.raftLeader = pollServer
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
