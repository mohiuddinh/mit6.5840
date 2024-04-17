package raft

func (rf *Raft) initiateAppendEntries() {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	
	if rf.currentState == Leader {
		for i := range rf.peers {
			if rf.me != i {
				startIdx := rf.getStartIdx()
				if rf.nextIdx[i] <= startIdx {
					DPrint("In initiateAppendEntries, sending snapshot from leader ", rf.me, " to follower ", i)
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIdx: startIdx, LastIncludedTerm: rf.getStartTerm(), Snapshot: rf.Persister.ReadSnapshot()}
					go rf.sendInstallSnapshot(i, &args, &InstallSnapshotReply{})
				} else {
					DPrint("In initiateAppendEntries, sending heartbeat from leader ", rf.me, " to follower ", i)
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
		DPrint("In sendAppendEntries, I am leader ", rf.me, " and received reply.Term > my term from ", server)
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
					DPrint("In sendAppendEntries, I am leader ", rf.me, " and follower is ", server, " and there is log conflict at index ", i + 1)
					return 
				}
			}
		}
	}

	if reply.Success {
		DPrint("In sendAppendEntries, I am leader ", rf.me, " and follower ", server, " and reply.Success is true")
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
				DPrint("In sendAppendEntries, I am leader ", rf.me, " and follower ", server, " and we want to update commitIdx to ", potentialCommitIdx)
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
		DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " but my term is higher")
    reply.Term = rf.currentTerm
    return
  }
	defer rf.persist(false, nil)

	rf.transitionToFollower(args.Term, false)
	reply.Term = rf.currentTerm
	DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " with term ", rf.currentTerm)

  if args.PrevLogIdx < rf.getStartIdx() {
		DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " but my startIdx > args.PrevLogIdx")
		reply.Term = 0
    return
  }

	if args.PrevLogIdx > rf.getLastIdx() {
		DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " but my lastIdx < args.PrevLogIdx")
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
		DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " but we have a conflictIdx ", reply.ConflictIdx, " and conflictterm ", reply.ConflictTerm)
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
	DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " and rpc was success so my new log is ", rf.log)
  newCommitIndex := min(args.LeaderCommit, rf.getLastIdx())
  if newCommitIndex > rf.commitIdx {
		DPrint("In AppendEntries, I am ", rf.me, " and received rpc from ", args.LeaderId, " and found newcommitidx ", newCommitIndex)
    rf.commitIdx = newCommitIndex
    rf.syncVar.Signal()
		// go rf.applyMsg()
  }

  reply.Success = true
}