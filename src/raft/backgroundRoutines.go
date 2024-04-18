package raft
// import "time"

func (rf *Raft) backgroundStateManager() {
	for !rf.killed() {
		select {
		case <-rf.sendHeartbeatTicker.C:
		rf.mu.Lock()
		if rf.currentState == Leader {
			DPrint("Timer ran out, kicking off initiateAppendEntries from leader ", rf.me)
			go rf.initiateAppendEntries()
		}
		rf.mu.Unlock()
		
		case <-rf.startElectionTicker.C:
			DPrint("Timer ran out, kicking off beginElection from server ", rf.me)
			rf.mu.Lock()
			rf.transitionToCandidate()
			go rf.beginElection()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) backgroundApply() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIdx <= rf.lastApplied {
			rf.syncVar.Wait()
		}

		// if rf.lastApplied >= rf.commitIndex {
		// 	rf.mu.Unlock() 
		// 	return 
		// }
		startIdx := rf.getStartIdx()
		commitIdx := rf.commitIdx
		copyLogs := deepcopySlice(rf.log[rf.lastApplied - startIdx + 1: rf.commitIdx - startIdx + 1])

		rf.mu.Unlock()

		for i := 0; i < len(copyLogs); i++ {
			commandApply := ApplyMsg{CommandValid: true, Command: copyLogs[i].Command, CommandIndex: copyLogs[i].Index, CommandTerm: copyLogs[i].Term}
			rf.applyCh <- commandApply
		}
		rf.mu.Lock()
		rf.lastApplied = max(commitIdx, rf.lastApplied)
		DPrint("In backgroundApply for server ", rf.me, " and updated lastApplied to ", rf.lastApplied)
		rf.mu.Unlock()
	}
}