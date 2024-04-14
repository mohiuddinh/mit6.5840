package shardctrler

import (
	"sync"
	"time"

	"sort"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	duplicateMap map[int64]Op 
	outgoingCommands map[int]chan *Op 

	configs []Config // indexed by config num
}

// type Command struct {
// 	CommandID int 
// 	Config Config 
// 	Err Err 
// 	WrongLeader bool 
// 	ClerkID int64 
// }

type OpName string 
const (
	LEAVE OpName = "LEAVE"
	MOVE OpName = "MOVE"
	JOIN OpName = "JOIN"
	QUERY OpName = "QUERY"
)

type Op struct {
	Servers map[int][]string 
	GIDs []int
	GID int 
	Shard int 
	Num int 

	ClerkID int64 
	CommandID int 
	Name OpName

	Config Config 
	Err Err 
	WrongLeader bool 
}

func (sc *ShardCtrler) isApplied(op Op) Op {
	command := Op{}
	sc.mu.Lock() 
	if sc.isDuplicate(op.ClerkID, op.CommandID) {
		command.Err = OUTDATED
		sc.mu.Unlock()
		return command
	}
	sc.mu.Unlock() 

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		command.WrongLeader = true 
		return command 
	}
	outChan := make(chan *Op, 1)

	sc.mu.Lock() 
	sc.outgoingCommands[index] = outChan
	sc.mu.Unlock() 

	select {
	case response := <-outChan: 
		command.Config = response.Config
		if response.ClerkID != op.ClerkID || response.CommandID != op.CommandID {
			command.WrongLeader = true 
			return command 
		} 
		command.Err = OK 
		command.WrongLeader = false  
		return command 
	case <-time.After(400*time.Millisecond): 
		command.Err = TIMEOUT
		return command 
	} 
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Name: JOIN, ClerkID: args.ClerkID, CommandID: args.CommandID, Servers: args.Servers}
	resp := sc.isApplied(op)
	reply.Err, reply.WrongLeader = resp.Err, resp.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Name: LEAVE, ClerkID: args.ClerkID, CommandID: args.CommandID, GIDs: args.GIDs}
	resp := sc.isApplied(op)
	reply.Err, reply.WrongLeader = resp.Err, resp.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Name: MOVE, ClerkID: args.ClerkID, CommandID: args.CommandID, GID: args.GID, Shard: args.Shard}
	resp := sc.isApplied(op)
	reply.Err, reply.WrongLeader = resp.Err, resp.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Name: QUERY, ClerkID: args.ClerkID, CommandID: args.CommandID, Num: args.Num}
	resp := sc.isApplied(op)
	reply.Err, reply.WrongLeader, reply.Config = resp.Err, resp.WrongLeader, resp.Config
}

func (sc *ShardCtrler) backgroundTask() {
	for {
		select {
		case msg := <- sc.applyCh: 
			sc.mu.Lock() 
			command := msg.Command.(Op)
			if command.Name == QUERY {
				sc.applyLocally(&command)
			} else if !sc.isDuplicate(command.ClerkID, command.CommandID) {
				sc.applyLocally(&command)
				sc.updateDuplicatedMap(command)
			}	
			sc.applyLocally(&command)
			sc.signal(command, msg.CommandIndex)
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) signal(op Op, commandIndex int) {
	if _, exists := sc.outgoingCommands[commandIndex]; exists {
		select {
		case <-sc.outgoingCommands[commandIndex]: 
		default: 
		}
		sc.outgoingCommands[commandIndex] <- &op
	}
}

func (sc *ShardCtrler) applyLocally(op *Op) {
	switch op.Name {
	case JOIN: 
		sc.addServers(op.Servers)
	case LEAVE: 
		sc.removeServers(op.GIDs)
	case MOVE: 	
		sc.moveShards(op.Shard, op.GID)
	case QUERY: 
		op.Config = sc.getConfig(op.Num)
	}
	op.Err = OK 
}

func (sc *ShardCtrler) moveShards(shard int, gid int) {
	if !(shard >= 0 && shard < NShards) {
		return
	}

	lastCfg := sc.configs[len(sc.configs)-1]
	if _, ok := lastCfg.Groups[gid]; !ok {
		return
	}

	newGroups := sc.mapCopy(lastCfg.Groups)
	newShards := lastCfg.Shards
	newShards[shard] = gid

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}

	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) getConfig(num int) Config {
	if num <= 1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) addServers(servers map[int][]string) {
	if len(servers) == 0 { 
		return 
	}
	oldConfig := sc.configs[len(sc.configs)-1]
	copyGroups := sc.mapCopy(oldConfig.Groups)

	// add new elements to it 
	for key, val := range servers {
		copyGroups[key] = sc.arrCopy(val)
	}

	shards := sc.loadBalanceShardsJoin(oldConfig.Shards, copyGroups)
	latestConfig := Config{Shards: shards, Groups: copyGroups, Num: oldConfig.Num + 1}

	sc.configs = append(sc.configs, latestConfig)
}

func (sc *ShardCtrler) loadBalanceShardsJoin(shards [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 { 
		return [NShards]int{}
	}
	// map that counts number of shards per GID 
	gidToShard := make(map[int][]int)
	for idx, gid := range shards {
		if _, ok := gidToShard[gid]; !ok {
			gidToShard[gid] = make([]int, 0)
		}
		gidToShard[gid] = append(gidToShard[gid], idx)
	}

	for { 
		smallest := minGIDShard(gidToShard)
		largest := maxGIDShard(gidToShard)
		if smallest != 0 && len(gidToShard[smallest]) - len(gidToShard[largest]) <= 1 {
			break 
		}
		gidToShard[largest] = gidToShard[largest][1:]
		gidToShard[smallest] = append(gidToShard[smallest], gidToShard[largest][0])
	}

	var balancedShards [NShards]int
	for gid, shards := range gidToShard {
		for _, shard := range shards {
			balancedShards[shard] = gid 
		}
	}
	return balancedShards
}

func (sc *ShardCtrler) removeServers(gids []int) {
	oldConfig := sc.configs[len(sc.configs)-1]
	group, balancedShards := sc.loadBalanceShardsLeave(oldConfig.Shards, oldConfig.Groups, gids)
	config := Config{Shards: balancedShards, Groups: group, Num: oldConfig.Num+1}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) loadBalanceShardsLeave(shards [NShards]int, groups map[int][]string, gids []int) (map[int][]string, [NShards]int) {
	// map that counts number of shards per GID 
	copyGroups := sc.mapCopy(groups)
	gidToShard := make(map[int][]int)
	for idx, gid := range shards {
		if _, ok := gidToShard[gid]; !ok {
			gidToShard[gid] = make([]int, 0)
		}
		gidToShard[gid] = append(gidToShard[gid], idx)
	}

	openShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := copyGroups[gid]; ok {
			delete(copyGroups, gid)
		}
		if val, ok := gidToShard[gid]; ok {
			openShards = append(openShards, val...)
			delete(gidToShard, gid)
		}
	}
	var balancedShards [NShards]int 
	if len(copyGroups) > 0 {
		for _, shard := range openShards {
			lowest := minGIDShard(gidToShard)
			gidToShard[lowest] = append(gidToShard[lowest], shard)
		}
		for gid, shards := range gidToShard {
			for _, shard := range shards {
				balancedShards[shard] = gid 
			}
		}
	}

	return copyGroups, balancedShards
}

func minGIDShard(gidToShard map[int][]int) int {
	gids := make([]int, len(gidToShard))
	for gid := range gidToShard {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	bestIndex := -1 
	smallest := NShards + 1 
	for _, gid := range gids {
		if gid != 0 && len(gidToShard[gid]) < smallest {
			bestIndex = gid 
			smallest = len(gidToShard[gid])
		}
	}
	return bestIndex
}

func maxGIDShard(gidToShard map[int][]int) int {
	// 0 is invalid and we need to assign everything to it 
	val, ok := gidToShard[0]
	if ok && len(val) > 0 {
		return 0 
	}
	gids := make([]int, len(gidToShard))
	for gid := range gidToShard {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	bestIndex := -1 
	largest := -1 
	for _, gid := range gids {
		if len(gidToShard[gid]) > largest {
			bestIndex = gid 
			largest = len(gidToShard[gid])
		}
	}
	return bestIndex
}


func (sc *ShardCtrler) arrCopy(this []string) []string {
	copyArr := make([]string, len(this))
	copy(copyArr, this)
	return copyArr
}

func (sc *ShardCtrler) mapCopy(this map[int][]string) map[int][]string {
	other := make(map[int][]string, len(this))
	for key, val := range this {
		other[key] = sc.arrCopy(val)
	}
	return other 
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) isDuplicate(clerkID int64, commandID int) bool {
	if command, ok := sc.duplicateMap[clerkID]; ok {
		if commandID <= command.CommandID {
			return true 
		}
	}
	return false 
}

// need lock 
func (sc *ShardCtrler) updateDuplicatedMap(op Op) {
	val, ok := sc.duplicateMap[op.ClerkID]
	if !ok || op.CommandID > val.CommandID {
		sc.duplicateMap[op.ClerkID] = op 
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.duplicateMap = make(map[int64]Op)
	sc.outgoingCommands = make(map[int]chan *Op)

	go sc.backgroundTask()

	return sc
}