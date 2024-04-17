package raft

import (
	"sync"
	"time"

	"6.5840/labrpc"
)

type AppendEntriesRPCArgs struct {
  Term         int     
  LeaderId     int     
  PrevLogIdx   int     
  PrevLogTerm  int     
  Entries     []LogInfo 
  LeaderCommit int   
}

type AppendEntriesRPCReply struct {
  Term    int  
  Success bool 
  ConflictTerm   int  
  ConflictIdx  int  
}

type InstallSnapshotArgs struct {
  Term              int    
  LeaderId          int    
  LastIncludedIdx   int    
  LastIncludedTerm  int    
  Snapshot          []byte 
}

type InstallSnapshotReply struct {
  Term int 
}

type RequestVoteArgs struct {
  Term         int 
  CandidateId  int 
  LastLogIdx   int 
  LastLogTerm  int 
}

type RequestVoteReply struct {
  Term        int  
  VoteGranted bool 
}

type RaftState int

const (
  Follower RaftState = iota
  Candidate
  Leader
)

type ApplyMsg struct {
  CommandValid bool
  Command      interface{}
  CommandIndex int
  CommandTerm  int

  SnapshotValid bool
  Snapshot      []byte
  SnapshotTerm  int
  SnapshotIndex int
}

type Raft struct {
  mu        sync.Mutex          // Lock to protect shared access to this peer's state
  peers     []*labrpc.ClientEnd // RPC end points of all peers
  Persister *Persister          // Object to hold this peer's persisted state
  me        int                 // this peer's index into peers[]
  dead      int32               // set by Kill()

  currentTerm int     
  votedFor    int     
  log        []LogInfo
  currentState       RaftState   

  startElectionTicker  *time.Ticker 
  sendHeartbeatTicker *time.Ticker 

  commitIdx  int   
  lastApplied  int   
  nextIdx    []int 
  matchIdx   []int 
  applyCh      chan ApplyMsg   

  voteCount int
  syncVar *sync.Cond 
}

type LogInfo struct {
  Command interface{} 
  Term    int         
  Index   int         
}