package raft

func (rf *Raft) initiateAppendEntries() {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	
	if rf.currentState == Leader {
		for i := range rf.peers {
			if rf.me != i {
				startIdx := rf.getStartIdx()
				if rf.nextIdx[i] <= startIdx {
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIdx: startIdx, LastIncludedTerm: rf.getStartTerm(), Snapshot: rf.Persister.ReadSnapshot()}
					go rf.sendInstallSnapshot(i, &args, &InstallSnapshotReply{})
				} else {
					args := AppendEntriesRPCArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIdx: rf.nextIdx[i] - 1, PrevLogTerm: rf.log[rf.nextIdx[i] - 1 - startIdx].Term, LeaderCommit: rf.commitIdx, Entries: deepcopySlice(rf.log[rf.nextIdx[i] - startIdx: ])}
					go rf.sendAppendEntries(i, &args, &AppendEntriesRPCReply{})
				}
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply)  {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	rf.mu.Lock() 
	defer rf.mu.Unlock()

	if !ok || rf.currentState != Leader || rf.currentTerm != args.Term {
		return 
	}

	if rf.currentTerm < reply.Term {
		rf.transitionToFollower(args.Term, true)
		rf.persist(false, nil)
		return 
	}

	if !reply.Success && reply.Term == rf.currentTerm {
		startIdx := rf.getStartIdx()
		rf.nextIdx[server] = max(rf.matchIdx[server] + 1, reply.ConflictIdx)
		
		if reply.ConflictTerm >= 0 {
			lower := max(startIdx, reply.ConflictIdx)
			for i := args.PrevLogIdx; i >= lower; i-- {
				if reply.ConflictTerm == rf.log[i - startIdx].Term {
					rf.nextIdx[server] = i + 1
					return 
				}
			}
		}
	}

	if reply.Success {
		rf.matchIdx[server] = max(rf.matchIdx[server], args.PrevLogIdx + len(args.Entries))
		rf.nextIdx[server] = rf.matchIdx[server] + 1
		startIdx := rf.log[0].Index
		for potentialCommitIdx := rf.getLastIdx(); potentialCommitIdx > rf.commitIdx; potentialCommitIdx-- {
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
				rf.syncVar.Signal() 
				// go rf.applyMsg()
				break
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

	reply.Success = false

  if args.Term < rf.currentTerm {
    reply.Term = rf.currentTerm
    return
  }
	defer rf.persist(false, nil)

	rf.transitionToFollower(args.Term, false)
	reply.Term = rf.currentTerm

  if args.PrevLogIdx < rf.getStartIdx() {
		reply.Term = 0
    return
  }

	if args.PrevLogIdx > rf.getLastIdx() {
		reply.ConflictIdx = rf.getLastIdx() + 1
		reply.ConflictTerm = -1
		return 
	}

	if rf.log[args.PrevLogIdx - rf.getStartIdx()].Term != args.PrevLogTerm {
		startIdx := rf.getStartIdx()
		conflictTerm := rf.log[args.PrevLogIdx-startIdx].Term
		index := args.PrevLogIdx - 1

		for i := args.PrevLogIdx - 1; i >= startIdx; i-- {
			if rf.log[index - startIdx].Term != conflictTerm {
				reply.ConflictIdx = i
				break
			}
		}
		reply.ConflictTerm = conflictTerm
		return
	}

	startIdx := rf.getStartIdx()
	foundIdx := -1
	for i := 0; i < len(args.Entries); i++ {
		logIdx := args.Entries[i].Index 
		logTerm := args.Entries[i].Term
		if logIdx >= startIdx + len(rf.log) || rf.log[logIdx - startIdx].Term != logTerm {
			rf.log = deepcopySlice(rf.log[:logIdx - startIdx])
			foundIdx = i 
			break 
		}
	}

	if foundIdx == -1 {
		foundIdx = len(args.Entries)
	}

  rf.log = append(rf.log, args.Entries[foundIdx: ]...)
  newCommitIndex := min(args.LeaderCommit, rf.getLastIdx())
  if newCommitIndex > rf.commitIdx {
    rf.commitIdx = newCommitIndex
    rf.syncVar.Signal()
		// go rf.applyMsg()
  }

  reply.Success = true
}