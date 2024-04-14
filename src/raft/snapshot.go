package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

func (rf *Raft) persist(setSnapshot bool, snapshot []byte) {
	if setSnapshot {
		rf.Persister.Save(rf.getCurrRaftState(), snapshot)
	} else {
		rf.Persister.Save(rf.getCurrRaftState(), rf.Persister.ReadSnapshot())
	}
}

func (rf *Raft) getCurrRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
  if data == nil || len(data) < 1 { 
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
    fmt.Println("decode persist state fail")
    return
  }

  rf.currentTerm = currentTerm
  rf.votedFor = votedFor
  rf.log = log
  rf.lastApplied = rf.getStartIdx()
  rf.commitIdx = rf.getStartIdx()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  startIdx := rf.getStartIdx()
  if startIdx >= index {
    return
  }

  rf.log = deepcopySlice(rf.log[index - startIdx:])
  rf.log[0].Command = nil

  rf.persist(true, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
  rf.mu.Lock()

  reply.Term = rf.currentTerm
  if args.Term < rf.currentTerm {
    rf.mu.Unlock()
    return
  }

  rf.transitionToFollower(args.Term, false)

  startIdx := rf.getStartIdx()
  if rf.commitIdx >= args.LastIncludedIdx {
    rf.mu.Unlock()
    return
  }

  if rf.getLastIdx() <= args.LastIncludedIdx {
    rf.log = make([]LogInfo, 1)
  } else {
    rf.log = deepcopySlice(rf.log[args.LastIncludedIdx - startIdx:])
  }

  rf.log[0] = LogInfo{Term: args.LastIncludedTerm, Index: args.LastIncludedIdx, Command: nil}
  rf.persist(true, args.Snapshot)

  rf.lastApplied = args.LastIncludedIdx
  rf.commitIdx = args.LastIncludedIdx

  rf.mu.Unlock()

  snapshotApply := ApplyMsg{SnapshotValid: true, Snapshot: args.Snapshot, SnapshotTerm: args.LastIncludedTerm,SnapshotIndex: args.LastIncludedIdx}

  // go func(apply ApplyMsg) {
  //   rf.applyCh <- apply
  // }(snapshotApply)

  rf.applyCh <- snapshotApply
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
  ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
  
  if !ok || rf.currentState != Leader || rf.currentTerm != args.Term {
    return 
  }

  if rf.currentTerm < reply.Term {
    rf.transitionToFollower(args.Term, true)
    return 
  }

  if rf.currentTerm == reply.Term {
    rf.matchIdx[server] = max(rf.matchIdx[server], args.LastIncludedIdx)
    rf.nextIdx[server] = rf.matchIdx[server] + 1
  }
}