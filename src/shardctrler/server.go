package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	// "fmt"
	"math"
	"sort"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	duplicateMap map[int64]Op 
	outgoingCommands map[int]chan *Op 

	configs []Config
}

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

	index, term, isLeader := sc.rf.Start(op)
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
		command.WrongLeader = response.WrongLeader 
		command.Err = response.Err 
		currTerm, isCurrLeader := sc.rf.GetState()
		if currTerm != term || !isCurrLeader {
			command.WrongLeader = true 
		}
		DPrint("In isApplied, received op ", op, " and response is ", command )
		return command 
	case <-time.After(400*time.Millisecond): 
		command.Err = TIMEOUT
		DPrint("In isApplied (timed out!), received op ", op, " and response is ", command )
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
		// fmt.Println(sc.configs)
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
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) addServers(servers map[int][]string) {
	if len(servers) == 0 {
		return
	}
	lastCfg := sc.configs[len(sc.configs)-1]
	groups := sc.mapCopy(lastCfg.Groups)
	newGroups := addMap(groups, servers)
	newShards := rebalanceShards(lastCfg.Shards, newGroups)

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) removeServers(gids []int) {
	if len(gids) == 0 {
		return
	}

	lastCfg := sc.configs[len(sc.configs)-1]
	newGroups := sc.mapCopy(lastCfg.Groups)
	shards := lastCfg.Shards

	for _, gid := range gids {
		for i, shard := range shards {
			if shard == gid {
				shards[i] = 0
			}
		}
		delete(newGroups, gid)
	}

	newShards := rebalanceShards(shards, newGroups)

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}

	sc.configs = append(sc.configs, newCfg)
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
	sc.configs[0].Num = 0 

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.duplicateMap = make(map[int64]Op)
	sc.outgoingCommands = make(map[int]chan *Op)

	go sc.backgroundTask()

	return sc
}

func addMap(m1 map[int][]string, m2 map[int][]string) map[int][]string {
	for k2, v2 := range m2 {
		m1[k2] = v2
	}

	return m1
}

func rebalanceShards(shardAllocations [NShards]int, nodeGroups map[int][]string) [NShards]int {
	// If there are no groups, return an empty allocation.
	if len(nodeGroups) == 0 {
		return [NShards]int{}
	}

	// Create a map to track which shards are owned by each group.
	groupShards := make(map[int][]int)
	for groupID := range nodeGroups {
		groupShards[groupID] = []int{}
	}
	for shardIndex, groupID := range shardAllocations {
		groupShards[groupID] = append(groupShards[groupID], shardIndex)
	}

	// Function to identify the group with the least shards.
	leastShardsGroup := func() int {
		minGroupID := -1
		minShardsCount := math.MaxInt32
		groupList := sortedKeys(groupShards)
		for _, groupID := range groupList {
			if groupID != 0 && len(groupShards[groupID]) < minShardsCount {
				minShardsCount = len(groupShards[groupID])
				minGroupID = groupID
			}
		}
		return minGroupID
	}

	// Function to identify the group with the most shards.
	mostShardsGroup := func() int {
		maxGroupID := -1
		maxShardsCount := math.MinInt32
		groupList := sortedKeys(groupShards)
		for _, groupID := range groupList {
			if groupID == 0 && len(groupShards[0]) > 0 {
				return 0
			}
			if len(groupShards[groupID]) > maxShardsCount {
				maxShardsCount = len(groupShards[groupID])
				maxGroupID = groupID
			}
		}
		return maxGroupID
	}

	for {
		// Identify groups with the minimum and maximum shard count.
		minGroupID, maxGroupID := leastShardsGroup(), mostShardsGroup()

		// Check whether rebalancing is needed or not.
		if maxGroupID != 0 && len(groupShards[maxGroupID])-len(groupShards[minGroupID]) <= 1 {
			break 
		}

		// Transfer a shard from the group with most shards to the one with least shards.
		shardToTransfer := groupShards[maxGroupID][0]
		shardAllocations[shardToTransfer] = minGroupID
		groupShards[maxGroupID] = groupShards[maxGroupID][1:]
		groupShards[minGroupID] = append(groupShards[minGroupID], shardToTransfer)
	}
	return shardAllocations
}

// Helper function to return map keys sorted in increasing order.
func sortedKeys(m map[int][]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}