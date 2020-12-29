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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"

import "time"
import "math/rand"

const (
	ELETIMEMIN = 300
	ELETIMEMAX = 600
	HEARTBEAT = 100
	RPCTIMEOUT = 50
)

const (
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
)
func StringRole(i int) string{
	switch i{
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	}
	return "ERROR"
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term int
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	role int
	setRoleCh chan bool
	applyCh chan ApplyMsg

	resetELETimer chan bool
	isKilled bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = false
	if (rf.role == LEADER){
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// DPrintf("Received RequestVote Node:%d, Term:%d, Role: %s, from Node:%d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), args.CandidateId, args.Term)
	// DPrintf("--%d \t %d \t %d", rf.votedFor, rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index)
	// DPrintf("--%d \t %d \t %d \t %d", args.Term, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else{
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.role = FOLLOWER
			rf.setRoleCh <- true
		}
		reply.Term = rf.currentTerm

		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (rf.log[len(rf.log)-1].Term < args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index <= args.LastLogIndex)){
			rf.resetELETimer <- true
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}else{
			reply.VoteGranted = false
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	tmpCh := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		tmpCh <- ok
	}()
	select {
	case ret := <- tmpCh:
		return ret
	case <-time.After(RPCTIMEOUT * time.Millisecond):
		return false
	}
	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// return ok
}

//----------------------
type AppendEntriesArgs struct {
	Term    int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entry []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	NextIndex int
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("Received AppendEntries Node:%d, Term:%d, Role: %s, from Node:%d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), args.LeaderId, args.Term)

	rf.mu.Lock()

	rf.resetELETimer <- true

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
	}else{
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = args.LeaderId
			if (rf.role != FOLLOWER){
				rf.role = FOLLOWER
				rf.setRoleCh <- true
			}
		}

		reply.Term = rf.currentTerm

		if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
			reply.NextIndex = len(rf.log)
		}else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			nextIndex := args.PrevLogIndex
			failTerm := rf.log[nextIndex].Term
			for rf.log[nextIndex].Term == failTerm {
				nextIndex--
			}
			reply.NextIndex = nextIndex + 1
		}else{
			reply.Success = true
			if len(args.Entry) != 0 {
				for i:=0; i < len(args.Entry); i++ {
					entry := args.Entry[i]
					curIndex := entry.Index
					if curIndex < len(rf.log) {
						if entry.Term != rf.log[curIndex].Term {
							rf.log = append(rf.log[:curIndex], entry)
						}
					}else {
						rf.log = append(rf.log, args.Entry[i:]...)
						break
					}
				}
				reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
			}else {
				reply.NextIndex = args.PrevLogIndex + 1
			}

			commitUntil := rf.commitIndex
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < reply.NextIndex - 1 {
					commitUntil = args.LeaderCommit
				}else {
					commitUntil = reply.NextIndex - 1
				}
			}
			for i:=rf.commitIndex+1 ; i <= commitUntil; i++ {
				rf.applyCh <- ApplyMsg{i, rf.log[i].Command, false, nil}
				rf.lastApplied = i
			}
			// if rf.commitIndex < commitUntil{
			// 	DPrintf("Received AppendEntries Node:%d, Term:%d, Role: %s CommittedIndex: %d", rf.me, rf.currentTerm, StringRole(rf.role), commitUntil)
			// }
			rf.commitIndex = commitUntil
		}
	}
	rf.persist()
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	tmpCh := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		tmpCh <- ok
	}()
	select {
	case ret := <- tmpCh:
		return ret
	case <-time.After(RPCTIMEOUT * time.Millisecond):
		return false
	}
	// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// return ok
}
//----------------------

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := false
	if rf.role == LEADER {
		isLeader = true
		entry := LogEntry{command, term, index}
		rf.log = append(rf.log, entry)
	}
	rf.persist()
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isKilled = true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))


	rf.role = FOLLOWER
	rf.setRoleCh = make(chan bool)
	rf.applyCh = applyCh
	rf.resetELETimer = make(chan bool)
	rf.isKilled = false
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())

	go rf.MainLoop()
	go rf.ElectionTimer()

	return rf
}

func (rf *Raft) MainLoop() {
	for !rf.isKilled {
		switch rf.role {
		case FOLLOWER:
			// DPrintf("MainLoop Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))
			<-rf.setRoleCh
		case CANDIDATE:
			// DPrintf("MainLoop Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))
			go rf.LeaderElection()
			<-rf.setRoleCh
		case LEADER:
			// DPrintf("MainLoop Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))
			go rf.LeaderFunc()
			<-rf.setRoleCh		
		}
	}
}
func (rf *Raft) ElectionTimer() {
	timer := time.NewTimer(10 * time.Millisecond)
	for !rf.isKilled{
		t := rand.Intn(ELETIMEMAX - ELETIMEMIN) + ELETIMEMIN
		timer.Reset(time.Duration(t) * time.Millisecond)
		select{
		case <- rf.resetELETimer:
		case <- timer.C:
			// DPrintf("ElectionTimer TimeOut Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))
			rf.role = CANDIDATE
			rf.setRoleCh <- true
		}
	}
}
func (rf *Raft) LeaderElection() {
	// DPrintf("LeaderElection Start Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))

	rf.mu.Lock()
	votingCh := make(chan bool, len(rf.peers))
	isVoting := true
	rf.currentTerm++
	rf.votedFor = rf.me
	votingCh <- true
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}
	rf.persist()
	rf.mu.Unlock()
	
	for i:=0; i<len(rf.peers); i++ {
		if (i == rf.me){
			continue
		}

		go func (i int) {
			if !isVoting {
				return
			}
			if rf.role != CANDIDATE {
				isVoting = false
				return
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, args, &reply) && isVoting {
				if args.Term < reply.Term {
					// DPrintf("LeaderElection ForcedQuit Node:%d, Term:%d, Role: %s // Node: %d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.Term)
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = FOLLOWER
					isVoting = false
					rf.persist()
					rf.mu.Unlock()
					return
				}else if reply.VoteGranted {
					// DPrintf("LeaderElection AgreeVote Node:%d, Term:%d, Role: %s // Node: %d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.Term)
					votingCh <- true
				}else if !reply.VoteGranted {
					// DPrintf("LeaderElection DisagreeVote Node:%d, Term:%d, Role: %s // Node: %d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.Term)
					votingCh <- false
				}
			} else if isVoting {
				// DPrintf("LeaderElection TimeOut-DisagreeVote Node:%d, Term:%d, Role: %s // Node: %d Term: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.Term)
				votingCh <- false
			}
		}(i)
	}

	agree := 0
	disagree := 0
	for isVoting {
		if rf.role != CANDIDATE{
			isVoting = false
		}
		select {
		case ok := <- votingCh:
			if ok {
				agree++
			}else{
				disagree++
			}
			if agree >= len(rf.peers)/2 + 1 {
				// DPrintf("LeaderElection Result:LEADER Node:%d, Term:%d, Role: %s agree: %d disagree: %d total: %d", rf.me, rf.currentTerm, StringRole(rf.role), agree, disagree, len(rf.peers))
                rf.mu.Lock()
				rf.role = LEADER
                rf.mu.Unlock()
				isVoting = false
			}else if disagree >= len(rf.peers)/2 + 1 {
				// DPrintf("LeaderElection Result:FOLLOWER Node:%d, Term:%d, Role: %s agree: %d disagree: %d total: %d", rf.me, rf.currentTerm, StringRole(rf.role), agree, disagree, len(rf.peers))
                rf.mu.Lock()
				rf.role = FOLLOWER
                rf.mu.Unlock()
				isVoting = false
			}else if agree + disagree == len(rf.peers){
				// DPrintf("LeaderElection Result:CANDIDATE Node:%d, Term:%d, Role: %s agree: %d disagree: %d total: %d", rf.me, rf.currentTerm, StringRole(rf.role), agree, disagree, len(rf.peers))
				// rf.role = CANDIDATE
				isVoting = false
			}
		}
	}
	time.Sleep(10*time.Millisecond)
	close(votingCh)
	// DPrintf("LeaderElection End Node:%d, Term:%d, Role: %s", rf.me, rf.currentTerm, StringRole(rf.role))
	rf.setRoleCh <- true
}
func (rf *Raft) LeaderFunc() {
    rf.mu.Lock()
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		rf.matchIndex[i] = 0
	}
    rf.mu.Unlock()

	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			go func() {
				for rf.role == LEADER {
					
					rf.mu.Lock()
					rf.resetELETimer <- true

					commitUntil := rf.commitIndex
					for j := len(rf.log)-1; rf.log[j].Index>rf.commitIndex && rf.log[j].Term==rf.currentTerm; j-- {
						cnt := 1
						for k:=0; k<len(rf.peers); k++ {
							if k != rf.me && rf.matchIndex[k] >= rf.log[j].Index {
								cnt++
							}
						}
						if cnt >= len(rf.peers)/2 + 1 {
							commitUntil = rf.log[j].Index
							break
						}
					}
					for j := rf.commitIndex + 1; j <= commitUntil; j++ {
						rf.applyCh <- ApplyMsg{j, rf.log[j].Command, false, nil}
					}
					// if rf.commitIndex < commitUntil{
					// 	DPrintf("LeaderFunc LeaderCommit Node:%d, Term:%d, Role: %s CommittedIndex: %d", rf.me, rf.currentTerm, StringRole(rf.role), commitUntil)
					// }
					rf.lastApplied = commitUntil
					rf.commitIndex = commitUntil

					// rf.persist()
					rf.mu.Unlock()

					time.Sleep(HEARTBEAT * time.Millisecond)
				}
			}()
		}else{
			go func(i int) {
				for rf.role == LEADER {
					rf.mu.Lock()
					args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, nil, rf.commitIndex}
					if rf.nextIndex[i] <  len(rf.log) {
						args.Entry = rf.log[rf.nextIndex[i]:]
					}
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					// DPrintf("LeaderFunc sendAppendEntries Node:%d, Term:%d, Role: %s, to Node: %d", rf.me, rf.currentTerm, StringRole(rf.role), i)
					if rf.sendAppendEntries(i, args, &reply) {
                        rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							// DPrintf("LeaderFunc sendAppendEntries Node:%d, Term:%d, Role: %s, to Node: %d, Term: %d // ForcedToBeFOLLOWER", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.Term)
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.role = FOLLOWER
							rf.setRoleCh <- true
							rf.persist()
						}else{
                            // rf.mu.Lock()
							if reply.Success {
								// DPrintf("LeaderFunc sendAppendEntries Node:%d, Term:%d, Role: %s, to Node: %d Success, NextIndex: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.NextIndex)
								rf.matchIndex[i] = reply.NextIndex - 1
								rf.nextIndex[i] = reply.NextIndex
							} else {
								// DPrintf("LeaderFunc sendAppendEntries Node:%d, Term:%d, Role: %s, to Node: %d NotSuccess, NextIndex: %d", rf.me, rf.currentTerm, StringRole(rf.role), i, reply.NextIndex)
								rf.nextIndex[i] = reply.NextIndex
							}
                            // rf.mu.Unlock()
						}
                        rf.mu.Unlock()
					}//else{
						// DPrintf("LeaderFunc sendAppendEntries Node:%d, Term:%d, Role: %s, to Node: %d TimeOut", rf.me, rf.currentTerm, StringRole(rf.role), i)
					//a}
					time.Sleep(HEARTBEAT * time.Millisecond)
				}
			}(i)
		}
	}
}
