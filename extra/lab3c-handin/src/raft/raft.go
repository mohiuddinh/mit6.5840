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
}

func (rf *Raft) isOtherLogUpToDate(otherLastLogIdx int, otherLastLogTerm int) bool {
	if rf.log[len(rf.log)-1].Term > otherLastLogTerm {
		return false 
	} else if otherLastLogTerm > rf.log[len(rf.log)-1].Term {
		return true 
	} 
	// same last term 
	return otherLastLogIdx >= (len(rf.log)-1)
}

func (rf *Raft) applyMessage() {
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	for i := rf.lastApplied + 1; i <= rf.commitIdx; i++ {
		rf.lastApplied = i
		DPrint("Applying message: rf.me message ", rf.me, ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i})
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	return rf.currentTerm, rf.currentState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes() 
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int 
		votedFor int 
		log []LogInfo
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil{	
		fmt.Println("Error while decoding!")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor 
		rf.log = log
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	defer rf.persist()
	reply.Term = rf.currentTerm 
	rf.currentState = Follower 
	rf.heartbeat <- true 
	DPrint(rf.me, " received heartbeat from " , args.LeaderId)
	rf.currentTerm = args.Term 
	
	if args.PrevLogIdx > (len(rf.log) - 1) {
		DPrint("in processappend, prevlogidx is too large for me: rf.me prevlogidx mylastidx ", rf.me, args.PrevLogIdx, len(rf.log)-1)
		reply.ConflictIdx = len(rf.log) // need to start matching at the end of the array
		return 
	}

	// check for log inconsistency 
	if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		thisTerm := rf.log[args.PrevLogIdx].Term
		for	i := args.PrevLogIdx; i >= 0; i-- {
			if rf.log[i].Term != thisTerm {
				break 
			} else {
				reply.ConflictIdx = i
			}
		}
		reply.ConflictTerm = thisTerm
		return 
	}

	// here everything is fine! 
	reply.Success = true 
	prev := rf.log[:args.PrevLogIdx+1]
	rf.log = append(prev, args.Entries...)

	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log) - 1)))
		go rf.applyMessage() 
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
		rf.persist() 
		return 
	}

	if reply.Success {
		DPrint("in sendappendentry, reply is success; my id is: ", rf.me, " response was from ", server, " and their nextidx and matchidx is ", rf.nextIdx, rf.matchIdx)
		rf.matchIdx[server] = int(math.Max(float64(rf.matchIdx[server]), float64(args.PrevLogIdx + len(args.Entries))))
		rf.nextIdx[server] = rf.matchIdx[server] + 1
	} else {
		// failed, need to retry 
		DPrint("in sendappendentry, reply is failure")
		if reply.ConflictTerm >= 0 {
			flag := false
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
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
		rf.matchIdx[server] = rf.nextIdx[server] - 1
	}

	for i := len(rf.log) - 1; i > rf.commitIdx; i-- {
		c := 1 
		if rf.log[i].Term == rf.currentTerm {
			for j, elem := range rf.matchIdx {
				if j != rf.me && elem >= i {
					DPrint(j, elem, rf.matchIdx, i)
					c += 1
				}
			}
		}
		if c > len(rf.peers) / 2 {
			rf.commitIdx = i 
			DPrint("got enough votes, applying messages ", rf.me, rf.commitIdx, rf.matchIdx)
			go rf.applyMessage() 
			break 
		}
	}
}

func (rf *Raft) InitiateAppendEntry() {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	if rf.currentState == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				sendEntry := make([]LogInfo, len(rf.log[rf.nextIdx[i]:]))
				copy(sendEntry, rf.log[rf.nextIdx[i]:])
				args := &AppendEntriesRPCArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIdx: rf.nextIdx[i]-1, PrevLogTerm: rf.log[rf.nextIdx[i]-1].Term, Entries: sendEntry, LeaderCommit: rf.commitIdx}
				DPrint(rf.me, " sending append entry to ", i)
				go rf.SendAppendEntry(i, args, &AppendEntriesRPCReply{})
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
	defer rf.persist() 
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term 
		rf.votedFor = -1 
		rf.currentState = Follower 
	}

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
			rf.persist()
		} else if reply.VoteGiven && rf.currentState == Candidate && rf.currentTerm == args.Term {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.currentState = Leader
				rf.wonElection <- true 
				rf.nextIdx = make([]int, len(rf.peers))
				rf.matchIdx = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIdx[i] = len(rf.log)
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
	
	rf.log = append(rf.log, LogInfo{Term: term, Command: command})
	rf.matchIdx[rf.me] = len(rf.log) - 1
	DPrint("In start, and am leader, so adding to my log", rf.me)
	rf.persist()
	return len(rf.log) - 1, term, isLeader
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
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIdx: len(rf.log)-1, LastLogTerm: rf.log[len(rf.log)-1].Term}
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
				rf.InitiateAppendEntry() 
				time.Sleep(100*time.Millisecond)
			case Candidate: 
				rf.mu.Lock() 
				rf.currentTerm++ 
				rf.votedFor = rf.me 
				rf.voteCount = 1 
				rf.mu.Unlock() 
				rf.persist()
				DPrint(rf.me, " became candidate")
				rf.BeginElection() 

				select {
					case <-time.After(time.Duration(150+rand.Int63()%300) * time.Millisecond): 
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
					case <-time.After(time.Duration(150+rand.Int63()%300) * time.Millisecond):
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
	rf.log = append(rf.log, LogInfo{Term: 0})

	rf.voteCount = 0
	rf.currentState = Follower 
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat = make(chan bool, 1)
	rf.wonElection = make(chan bool, 1)
	rf.gaveVote = make(chan bool, 1)
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
