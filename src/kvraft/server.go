package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	// "fmt"
)

const debug = false
func DPrint(a ...interface{})  {
	if debug {
		log.Println(a...)
	}
}


type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string 
	Key string
	Value string 
	ClerkId int64 
	CommandId int64
	RaftStatus string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	outgoingCommands map[int]chan Command
	data map[string]string
	clientTable map[int64]int64
}

func (kv *KVServer) isApplied(command Command) (bool, Command) {
	index, _, isLeader := kv.rf.Start(command)
	DPrint("In isApplied with command ", command, " and index and isLeader ", index, isLeader)
	if !isLeader {	// can't apply since not leader
		return false, Command{RaftStatus: ErrWrongLeader, Value: ""}
	}

	kv.mu.Lock()
	_, ok := kv.outgoingCommands[index]
	if !ok {
		kv.outgoingCommands[index] = make(chan Command, 1)
	}
	kv.mu.Unlock()

	select {
	case res := <-kv.outgoingCommands[index]: 
		DPrint("In isApplied and channel returned some old and new command ", command, res)
		isSame := command.ClerkId == res.ClerkId && command.CommandId == res.CommandId
		if !isSame {
			return false, Command{RaftStatus: ErrNotApplied, Value: ""}
		}
		res.RaftStatus = OK
		return true, res
	case <-time.After(200*time.Millisecond): // time out and return false
		DPrint("in isApplied with command ", command, " and timed out")
		return false, Command{RaftStatus: ErrTimeout, Value: ""}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Command{Type: GET, Key: args.Key, ClerkId: args.ClerkId, CommandId: args.CommandId}
	DPrint("In KVServ with Get and command ", command, " calling isApplied now")
	ok, resCommand := kv.isApplied(command)
	DPrint("In KVServ with Get and isApplied returned with ", ok, resCommand)
	reply.Status = resCommand.RaftStatus
	reply.Value = resCommand.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Command{Type: args.Type, Key: args.Key, ClerkId: args.ClerkId, CommandId: args.CommandId, Value: args.Value}
	DPrint("In KVServ with PutApend and command ", command, " calling isApplied now")
	_, resCommand := kv.isApplied(command)
	DPrint("In KVServ with PutAppend and isApplied returned with ", resCommand)
	reply.Status = resCommand.RaftStatus
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// must hold lock
func (kv *KVServer) applyLocally(command *Command) string {
	switch command.Type {
	case PUT: 
		kv.data[command.Key] = command.Value
	case APPEND: 
		if value, exists := kv.data[command.Key]; exists {
			kv.data[command.Key] = value + command.Value 
		} else {
			kv.data[command.Key] = command.Value
		}
	case GET: 
		if value, exists := kv.data[command.Key]; exists {
			command.Value = value
		} else {
			command.RaftStatus = ErrNoKey
		}
	}
	DPrint("In applyLocally and applied command ", command, " now data is ", kv.data)
	return ""
}

func (kv *KVServer) backgroundTask() {
	for {
		applyMsg := <-kv.applyCh
		kv.mu.Lock() 
		
		if !applyMsg.SnapshotValid {
			// fmt.Println(applyMsg)
			if applyMsg.Command == nil {
				applyMsg.Command = Command{}
			}
			resultCommand := applyMsg.Command.(Command)
			DPrint("Received message not it's not snapshot ", resultCommand)
			if resultCommand.Type == GET {
				kv.applyLocally(&resultCommand)
			} else {
				val, ok := kv.clientTable[resultCommand.ClerkId]
				if !ok || resultCommand.CommandId > val { // check if duplicate 
					kv.applyLocally(&resultCommand)
					kv.clientTable[resultCommand.ClerkId] = resultCommand.CommandId
				}
			}
			
			if _, exists := kv.outgoingCommands[applyMsg.CommandIndex]; !exists {
				kv.outgoingCommands[applyMsg.CommandIndex] = make(chan Command, 1)
				DPrint("Index exists and it's ", applyMsg.CommandIndex, " and we send value to channel")
			} else { // clear out channel 
				select {
				case <-kv.outgoingCommands[applyMsg.CommandIndex]: 
				default: 
				}
			}
			kv.outgoingCommands[applyMsg.CommandIndex] <- resultCommand // fill in channel! 
		} else { // snapshot
			DPrint("Received snapshot")
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)
			d.Decode(&kv.data)
			d.Decode(&kv.clientTable)
		}
		
		if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			// if curr raft size > 90% of max allowed, we call snapshot 
			DPrint("State is too big, now snapshotting")
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.clientTable)
			go kv.rf.Snapshot(applyMsg.CommandIndex, w.Bytes())
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) recoverFromSnapshot() {
	snapshot := kv.rf.Persister.ReadSnapshot()
	if len(snapshot) > 0 { 
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.data)
		d.Decode(&kv.clientTable)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.outgoingCommands = make(map[int]chan Command)
	kv.clientTable = make(map[int64]int64)

	kv.recoverFromSnapshot()	

	go kv.backgroundTask()

	return kv
}
