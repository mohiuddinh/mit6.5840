package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "fmt"
	"log"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const debug = false
func DPrint(a ...interface{})  {
	if debug {
		log.Println(a...)
	}
}

type RaftState int 

const (
    Follower RaftState = iota
    Candidate
    Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogInfo struct {
	Index int
	Term int 
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 
	votedFor int 
	log []LogInfo
	commitIdx int 
	lastApplied int 

	voteCount int 
	currentState RaftState 
	heartbeat chan bool 
	wonElection chan bool 
	gaveVote chan bool 
	applyCh chan ApplyMsg

	nextIdx []int 
	matchIdx []int 

	sendLogs chan bool 
	heartbeatTimer *time.Timer
}

func (rf *Raft) isOtherLogUpToDate(otherLastLogIdx int, otherLastLogTerm int) bool {
	if rf.getLastTerm() > otherLastLogTerm {
		return false 
	} else if otherLastLogTerm > rf.getLastTerm() {
		return true 
	} 
	// same last term 
	return otherLastLogIdx >= rf.getLastIndex()
}

func (rf *Raft) applyMessage() {
	rf.mu.Lock() 
	startIdx := rf.log[0].Index
	lastApplied := rf.lastApplied 
	commitIdx := rf.commitIdx
	me := rf.me 
	cpy := make([]LogInfo, len(rf.log))
	copy(cpy, rf.log)
	rf.mu.Unlock() 

	for i := lastApplied + 1; i <= commitIdx; i++ {
		// fmt.Println("Applying message: i baseIdx rf.me message", i, startIdx, me, lastApplied)
		DPrint("Applying message: i baseIdx rf.me message cpylog", i, startIdx, me, ApplyMsg{CommandValid: true, Command: cpy[i - startIdx].Command, CommandIndex: i}, cpy)
		
		command := cpy[i - startIdx].Command
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: i}
		rf.mu.Lock()
		rf.lastApplied = i
		rf.mu.Unlock()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	return rf.currentTerm, rf.currentState == Leader
}

func (rf *Raft) getCurrRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(setSnapshot bool, snapshot []byte) {
	if setSnapshot {
		rf.persister.Save(rf.getCurrRaftState(), snapshot)
	} else {
		rf.persister.Save(rf.getCurrRaftState(), rf.persister.ReadSnapshot())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int 
		votedFor int 
		log []LogInfo
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {	
		fmt.Println("Error while decoding!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor 
		rf.log = log
		rf.commitIdx = log[0].Index
		rf.lastApplied = log[0].Index
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Println("in installSnapshot, server rf.commitIdx args.LastIncludedIdx", rf.me, rf.commitIdx, args.LastIncludedIndex)
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return 
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentState = Follower
		rf.persist(false, nil)
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIdx {
		rf.snapshotLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIdx = args.LastIncludedIndex
		rf.persist(true, args.Data)
		// fmt.Println("getting caught up! ", rf.log)

		snapshotApply := ApplyMsg{SnapshotValid: true, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex, Snapshot: args.Data}

		go func(apply ApplyMsg) {
        rf.applyCh <- apply
    }(snapshotApply)
	}
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Println("in sendinstallsnapshot, server otherserver", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.currentState != Leader || args.Term != rf.currentTerm{
		return 
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.currentState = Follower
		rf.persist(false, nil)
		return
	}

	rf.nextIdx[server] = args.LastIncludedIndex + 1
	rf.matchIdx[server] = args.LastIncludedIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// hold lock while doing this
func (rf *Raft) snapshotLog(lastIndex int, lastTerm int) {
	startIdx := -1 
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIndex && rf.log[i].Term == lastTerm {
			startIdx = i 
			break 
		}
	}

	newlog := []LogInfo{{Index: lastIndex, Term: lastTerm}}
	if startIdx != -1 {
		newlog = append(newlog, rf.log[startIdx + 1:]...)
	}
	rf.log = newlog
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock() 
	defer rf.mu.Unlock() 
	defer rf.persist(true, snapshot) 

	startIdx := rf.log[0].Index
	if startIdx >= index || index > rf.getLastIndex() {
		return // skip this request 
	}
	rf.snapshotLog(index, rf.log[index - startIdx].Term)
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 
	CandidateId int 
	LastLogIdx int 
	LastLogTerm int 
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 
	VoteGiven bool 
}

type AppendEntriesRPCArgs struct {
	Term int 
	LeaderId int 
	PrevLogIdx int 
	PrevLogTerm int

	Entries []LogInfo 
	LeaderCommit int 
}

type AppendEntriesRPCReply struct {
	Term int 
	Success bool 
	ConflictIdx int 
	ConflictTerm int 
}

func (rf *Raft) ProcessAppendEntry(args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply) {
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	reply.ConflictIdx = -1 
	reply.ConflictTerm = -1
	reply.Success = false 

	if args.Term < rf.currentTerm {
		DPrint("in processappend, my term is bigger: ", rf.me)
		reply.Term = rf.currentTerm 
		return 
	} 
	defer rf.persist(false, nil)
	rf.currentState = Follower 
	rf.heartbeat <- true 
	DPrint(rf.me, " received heartbeat from (in appendentries)" , args.LeaderId)
	rf.currentTerm = args.Term 
	reply.Term = rf.currentTerm 
	
	if args.PrevLogIdx > rf.getLastIndex() {
		DPrint("in processappend, prevlogidx is too large for me: rf.me prevlogidx mylastidx ", rf.me, args.PrevLogIdx, rf.getLastIndex())
		reply.ConflictIdx = rf.getLastIndex() + 1 // need to start matching at the end of the array
		return 
	}

	startIdx := rf.log[0].Index
	if args.PrevLogIdx >= startIdx && args.PrevLogTerm != rf.log[args.PrevLogIdx - startIdx].Term {
		DPrint("Conflict and need to find the index and term to send back, rf.me ", rf.me)
		thisTerm := rf.log[args.PrevLogIdx].Term
		for i := args.PrevLogIdx - 1; i >= startIdx; i-- {
			if rf.log[i - startIdx].Term != thisTerm {
				reply.ConflictIdx = i + 1
				break
			}
		}
		reply.ConflictTerm = thisTerm
	} else if args.PrevLogIdx > startIdx - 2 { // prevlogidx must be atleast startidx - 1 or more
		// DPrint("Success in processappendentry, rf.me ", rf.me)
		rf.log = rf.log[:args.PrevLogIdx - startIdx + 1]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		if rf.commitIdx < args.LeaderCommit {
			// DPrint("Committing, rf.me commitIdx", rf.me, rf.commitIdx)
			rf.commitIdx = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastIndex())))
			go rf.applyMessage()
		}
	}
}

func (rf *Raft) SendAppendEntry(server int, args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply) {
	ok := rf.peers[server].Call("Raft.ProcessAppendEntry", args, reply)

	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	if !ok || rf.currentState != Leader || reply.Term != args.Term {
		DPrint("in sendappendentry, received bad info and returning early: ", rf.me)
		return 
	}

	if reply.Term > rf.currentTerm {
		DPrint("in sendappendentry, received reply term greater than mine, becoming follower: ", rf.me)
		rf.currentTerm = reply.Term 
		rf.currentState = Follower 
		rf.votedFor = -1
		rf.persist(false, nil) 
		return 
	}

	if reply.Success {
		DPrint("in sendappendentry, reply is success; my id is: ", rf.me, " response was from ", server, " and their nextidx and matchidx is ", rf.nextIdx, rf.matchIdx)
		if len(args.Entries) > 0 { // success and can update
			// DPrint("IN sendappendentry and successfully added to follower log, rf.me follower", rf.me, server)
			rf.nextIdx[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIdx[server] = rf.nextIdx[server] - 1
		}
	} else {
		// failed, need to retry 
		DPrint("in sendappendentry, reply is failure")
		startIdx := rf.log[0].Index
		if reply.ConflictTerm >= 0 {
			flag := false
			for i := rf.getLastIndex(); i > startIdx; i-- {
				if rf.log[i - startIdx].Term == reply.ConflictTerm {
					rf.nextIdx[server] = i + 1
					flag = true 
					break 
				}
			}
			if !flag {
				rf.nextIdx[server] = reply.ConflictIdx
			}
		} else { // ConflictTerm == -1, follower log was small
			rf.nextIdx[server] = reply.ConflictIdx
		}
		rf.matchIdx[server] = int(math.Max(float64(startIdx), float64(rf.nextIdx[server] - 1)))
	}

	startIdx := rf.log[0].Index
	for potentialCommitIdx := rf.getLastIndex(); potentialCommitIdx > rf.commitIdx; potentialCommitIdx-- {
		count := 1
		if rf.log[potentialCommitIdx - startIdx].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIdx[i] >= potentialCommitIdx {
					count++
				}
			}
		}	
		if count > len(rf.peers)/2 {
			rf.commitIdx = potentialCommitIdx
			// DPrint("got enough votes, applying messages ", rf.me, rf.commitIdx, rf.matchIdx)
			go rf.applyMessage()
			break
		}
	}
}

func (rf *Raft) InitiateAppendEntry() {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	if rf.currentState == Leader {
		startIdx := rf.log[0].Index

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if rf.nextIdx[i] <= startIdx { // need to send snapshot to follower because lagging too far behind 
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.log[0].Index,LastIncludedTerm: rf.log[0].Term, Data: rf.persister.ReadSnapshot()}
					go rf.SendInstallSnapshot(i, &args, &InstallSnapshotReply{})
				} else {
					args := &AppendEntriesRPCArgs{LeaderId: rf.me, Term: rf.currentTerm, PrevLogIdx: rf.nextIdx[i] - 1, LeaderCommit: rf.commitIdx}
					// bounds checking
					if rf.nextIdx[i] <= rf.getLastIndex() {
						args.Entries = rf.log[rf.nextIdx[i] - startIdx:]
					}
					if args.PrevLogIdx >= startIdx {
						args.PrevLogTerm = rf.log[args.PrevLogIdx - startIdx].Term
					}
					go rf.SendAppendEntry(i, args, &AppendEntriesRPCReply{})
				}
			}
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	reply.VoteGiven = false

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm  
		return 
	} 
	defer rf.persist(false, nil) 
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term 
		rf.votedFor = -1 
		rf.currentState = Follower 
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isOtherLogUpToDate(args.LastLogIdx, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGiven = true 
		rf.gaveVote <- true 
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock() 
	defer rf.mu.Unlock() 
	
	if ok {
		if rf.currentTerm < reply.Term {
			rf.votedFor = -1
			rf.currentTerm = reply.Term 
			rf.currentState = Follower 
			rf.persist(false, nil)
		} else if reply.VoteGiven && rf.currentState == Candidate && rf.currentTerm == args.Term {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.currentState = Leader
				rf.heartbeatTimer = time.NewTimer(60*time.Millisecond)
				rf.wonElection <- true 
				rf.nextIdx = make([]int, len(rf.peers))
				rf.matchIdx = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIdx[i] = rf.getLastIndex() + 1
				}
			}
		}
	}
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.currentState == Leader

	if !isLeader {
		DPrint("In start, but not leader, so returning: ", rf.me)
		return index, term, isLeader
	}
	lastIdx := rf.getLastIndex()
	rf.log = append(rf.log, LogInfo{Index: lastIdx + 1, Term: term, Command: command})
	DPrint("In start, and am leader, so adding to my log: server ", rf.me, " and matchidx and nextidx is ", rf.matchIdx, rf.nextIdx)
	rf.persist(false, nil)

	select { // empty in case there is something so we don't block 
	case <-rf.sendLogs: // we want to immediately send this to be circulated to the other follows for efficient processing
	default: 
	}
	rf.sendLogs <- true
	return lastIdx + 1, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) BeginElection() {
	rf.mu.Lock() 
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIdx: rf.getLastIndex(), LastLogTerm: rf.getLastTerm()}
	// DPrint(rf.Me, " starting elections with args: ", args)
	stateNow := rf.currentState 
	rf.mu.Unlock() 
	if stateNow == Candidate {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {	
				go rf.SendRequestVote(i, args, &RequestVoteReply{})
			}
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.currentState {
			case Leader: 
				DPrint(rf.me, " is leader and sending heartbeat")
				// rf.InitiateAppendEntry() 
				// time.Sleep(30*time.Millisecond)
				select {
				case <-rf.heartbeatTimer.C: 
					rf.InitiateAppendEntry()
					rf.heartbeatTimer.Reset(80*time.Millisecond)
				case <-rf.sendLogs: // send logs immediately but wait a bit so things catch up -- otherwise bugs? 
					time.Sleep(30*time.Millisecond)
					rf.InitiateAppendEntry()
					rf.heartbeatTimer.Reset(80*time.Millisecond)
				}
			case Candidate: 
				rf.mu.Lock() 
				rf.currentTerm++ 
				rf.votedFor = rf.me 
				rf.voteCount = 1 
				rf.mu.Unlock() 
				rf.persist(false, nil)
				DPrint(rf.me, " became candidate")
				rf.BeginElection() 

				select {
					case <-time.After(time.Duration(300+rand.Int63()%300) * time.Millisecond): 
						DPrint(rf.me, " became candidate and timed out, so restarting election")
					case <-rf.wonElection:
						DPrint(rf.me, " became candidate and won")
					case <-rf.heartbeat: 
						rf.mu.Lock() 
						DPrint(rf.me, " became candidate but received a heartbeat, so now is follower")
						rf.currentState = Follower 
						rf.mu.Unlock() 
				}
			case Follower: 
				select {
					case <-time.After(time.Duration(300+rand.Int63()%300) * time.Millisecond):
						rf.mu.Lock() 
						DPrint(rf.me, " is follower and timed out, so becoming candidate")
						rf.currentState = Candidate
						rf.mu.Unlock()
					case <-rf.heartbeat: 
						// DPrint(rf.me, " received heartbeat")
					case <-rf.gaveVote: 
						DPrint(rf.me, " gave vote to ", rf.votedFor)
				}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogInfo{Index: 0, Term: 0})

	rf.voteCount = 0
	rf.currentState = Follower 
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat = make(chan bool, 50)
	rf.wonElection = make(chan bool, 50)
	rf.gaveVote = make(chan bool, 50)
	rf.applyCh = applyCh

	rf.sendLogs = make(chan bool, 1) // TODO

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist(false, nil)

	// start ticker goroutine to start elections
	// fmt.Println("starting rf.ticker() and server ", rf.me)
	go rf.ticker()

	return rf
}