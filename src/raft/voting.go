package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	if ok {
		if rf.currentTerm < reply.Term {
			DPrint("In sendRequestVote, and I am ", rf.me, " but received higher term from server ", server, " , so returning early")
			rf.transitionToFollower(reply.Term, true)
			rf.persist(false, nil)
		} else if reply.VoteGranted && rf.currentState == Candidate && rf.currentTerm == args.Term {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				DPrint("In sendRequestVote, I am ", rf.me, " and I received majority votes so I am becoming leader")
				rf.transitionToLeader()
				go rf.initiateAppendEntries()
			}
		}
	}
  return ok
}

func (rf *Raft) beginElection() {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	rf.currentTerm += 1 
	rf.votedFor = rf.me 
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIdx: rf.getLastIdx(), LastLogTerm: rf.getLastTerm()}
	rf.voteCount = 1
	rf.persist(false, nil)

	for i := range rf.peers {
		if rf.me != i {
			DPrint("In beginElection, and I am ", rf.me, " and sending request to server ", i)
			go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock() 
	defer rf.mu.Unlock() 

	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		DPrint("In RequestVote, and I am ", rf.me, " and my term number is higher so not granting vote to ", args.CandidateId)
		reply.Term = rf.currentTerm  
		return 
	} 
	defer rf.persist(false, nil) 
	
	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term, true)
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isOtherLogUpToDate(args.LastLogIdx, args.LastLogTerm) {
		DPrint("In RequestVote, and I am ", rf.me, " and I am granting vote to ", args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true 
		rf.startElectionTicker.Reset(time.Duration(300+rand.Int63()%300) * time.Millisecond)
    rf.sendHeartbeatTicker.Stop()
	}
}