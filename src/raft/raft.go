package raft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
  "math/rand"
)

func (rf *Raft) GetState() (int, bool) {
  rf.mu.Lock() 
	defer rf.mu.Unlock() 

	return rf.currentTerm, rf.currentState == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
  rf.mu.Lock() 
	defer rf.mu.Unlock()
	isLeader := rf.currentState == Leader

	if !isLeader {
		return -1, rf.currentTerm, isLeader
	}
  DPrint("In Start, I am ", rf.me, " received command ", command)
  rf.log = append(rf.log, LogInfo{Index: rf.getLastIdx() + 1, Term: rf.currentTerm, Command: command})
  rf.persist(false, nil)

	go rf.initiateAppendEntries()
  return rf.getLastIdx(), rf.currentTerm, isLeader
}

func (rf *Raft) Kill() {
  atomic.StoreInt32(&rf.dead, 1)
  // Your code here, if desired.
}

func (rf *Raft) killed() bool {
  z := atomic.LoadInt32(&rf.dead)
  return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
  Persister *Persister, applyCh chan ApplyMsg) *Raft {
  rf := &Raft{}
  rf.peers = peers
  rf.Persister = Persister
  rf.me = me

  rf.currentTerm = 0
  rf.votedFor = -1
  rf.currentState = Follower

  rf.startElectionTicker = time.NewTicker(time.Duration(300+rand.Int63()%300) * time.Millisecond)
  rf.sendHeartbeatTicker = time.NewTicker(80 * time.Millisecond)
  rf.sendHeartbeatTicker.Stop()
  
  rf.log = make([]LogInfo, 1) 
  rf.nextIdx = make([]int, len(peers))
  rf.matchIdx = make([]int, len(peers))
  rf.applyCh = applyCh

	rf.voteCount = 0
	rf.syncVar = sync.NewCond(&rf.mu)
  rf.newCommitCh = make(chan struct{}, 1)

  rf.readPersist(Persister.ReadRaftState())
  DPrint("In make, creating server ", rf.me)

  go rf.backgroundStateManager()
  go rf.backgroundApply()
  return rf
}

