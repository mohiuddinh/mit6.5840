package raft

import (
	"log"
	"math/rand"
	"time"
)

const debug = false

func DPrint(a ...interface{})  {
	if debug {
		log.Println(a...)
	}
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) isOtherLogUpToDate(otherLastLogIdx int, otherLastLogTerm int) bool {
	if rf.getLastTerm() > otherLastLogTerm {
		return false 
	} else if otherLastLogTerm > rf.getLastTerm() {
		return true 
	} 
	// same last term 
	return otherLastLogIdx >= rf.getLastIdx()
}

func deepcopySlice(src []LogInfo) []LogInfo {
    dst := make([]LogInfo, len(src))
    copy(dst, src)
    return dst
}

func (rf *Raft) transitionToFollower(term int, updateVotedFor bool) {
	rf.startElectionTicker.Reset(time.Duration(300+rand.Int63()%300) * time.Millisecond)
	rf.sendHeartbeatTicker.Stop() 
	if updateVotedFor {
		rf.votedFor = -1 
	}
	rf.currentTerm = term 
	rf.currentState = Follower
}

func (rf *Raft) transitionToCandidate() {
	rf.currentState = Candidate 
	rf.startElectionTicker.Reset(time.Duration(300+rand.Int63()%300) * time.Millisecond)
}

func (rf *Raft) transitionToLeader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIdx[i] = 0
		rf.nextIdx[i] = rf.getLastIdx() + 1
	}
	rf.startElectionTicker.Stop() 
	rf.sendHeartbeatTicker.Reset(80 * time.Millisecond)
	rf.currentState = Leader
}

func (rf *Raft) getStartIdx() int {
	return rf.log[0].Index
}

func (rf *Raft) getStartTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) getLastIdx() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}