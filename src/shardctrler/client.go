package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	clerkID int64
	commandID int
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
	ck.clerkID = nrand() 
	ck.raftLeader = 0
	ck.commandID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock() 
	pollServer := ck.raftLeader
	args := &QueryArgs{Num: num, ClerkID: ck.clerkID, CommandID: ck.commandID}
	ck.commandID++
	ck.mu.Unlock() 

	for {
		reply := &QueryReply{}
		DPrint("Sending QUERY... ckID: ", ck.clerkID, "pollServer: ", pollServer, "Op: QUERY args: ", args)
		ok := ck.servers[pollServer].Call("ShardCtrler.Query", args, reply)
		if ok && !reply.WrongLeader && reply.Err != TIMEOUT {
			ck.mu.Lock() 
			DPrint("Received QUERY response... ckId: ", ck.clerkID, "pollServer: ", pollServer, "Op: QUERY args: ", args, " config: ", reply.Config)
			ck.raftLeader = pollServer
			ck.mu.Unlock() 
			return reply.Config
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock() 
	args := &JoinArgs{Servers: servers, ClerkID: ck.clerkID, CommandID: ck.commandID}
	pollServer := ck.raftLeader
	ck.commandID++
	ck.mu.Unlock()

	for {
		reply := &JoinReply{}
		DPrint("Sending JOIN... ckID: ", ck.clerkID, "pollServer: ", pollServer, "Op: JOIN args: ", args)
		ok := ck.servers[pollServer].Call("ShardCtrler.Join", args, reply)
		if ok && !reply.WrongLeader && reply.Err != TIMEOUT {
			ck.mu.Lock() 
			DPrint("Received JOIN response... ckId: ", ck.clerkID, "pollServer: ", pollServer, "Op: JOIN args: ", args)
			ck.raftLeader = pollServer
			ck.mu.Unlock() 
			return 
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock() 
	args := &LeaveArgs{GIDs: gids, ClerkID: ck.clerkID, CommandID: ck.commandID}
	pollServer := ck.raftLeader
	ck.commandID++
	ck.mu.Unlock()

	for {
		reply := &LeaveReply{}
		DPrint("Sending LEAVE... ckID: ", ck.clerkID, "pollServer: ", pollServer, "Op: LEAVE args: ", args)
		ok := ck.servers[pollServer].Call("ShardCtrler.Leave", args, reply)
		if ok && !reply.WrongLeader && reply.Err != TIMEOUT {
			ck.mu.Lock() 
			DPrint("Received LEAVE response... ckId: ", ck.clerkID, "pollServer: ", pollServer, "Op: LEAVE args: ", args)
			ck.raftLeader = pollServer
			ck.mu.Unlock() 
			return 
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock() 
	args := &MoveArgs{GID: gid, Shard: shard, ClerkID: ck.clerkID, CommandID: ck.commandID}
	pollServer := ck.raftLeader
	ck.commandID++
	ck.mu.Unlock()

	for {
		reply := &MoveReply{}
		DPrint("Sending MOVE... ckID: ", ck.clerkID, "pollServer: ", pollServer, "Op: MOVE args: ", args)
		ok := ck.servers[pollServer].Call("ShardCtrler.Move", args, reply)
		if ok && !reply.WrongLeader && reply.Err != TIMEOUT {
			ck.mu.Lock() 
			DPrint("Received MOVE response... ckId: ", ck.clerkID, "pollServer: ", pollServer, "Op: MOVE args: ", args)
			ck.raftLeader = pollServer
			ck.mu.Unlock() 
			return 
		}
		pollServer = (pollServer + 1) % len(ck.servers)
	}
}
