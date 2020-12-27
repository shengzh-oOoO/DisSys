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
	ELETIMEMIN = 150
	ELETIMEMAX = 300
	HEARTBEAT = 100
)

const (
	Follower = 1
	Candidate = 2
	Leader = 3
)

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
	Term    int
	Index	int
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
	role int

	nextIndex []int
	matchIndex []int

	chanRole chan bool
	chanCommitted chan ApplyMsg

	chanHeartbeat chan bool
	isKilled bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// var term int
	// var isleader bool
	// // Your code here.
	// return term, isleader
	return rf.currentTerm, rf.role == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.chanRole <- true
	}

	reply.Term = args.Term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (rf.log[len(rf.log)-1].Term < args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index <= args.LastLogIndex)){
		rf.chanHeartbeat <- true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}else{
		reply.VoteGranted = false
	}
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
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(HEARTBEAT * time.Millisecond):
		ret = false
	}
	return ret
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.chanHeartbeat <- true
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.chanRole <- true
	}

	reply.Term = args.Term

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		nextIndex := args.PrevLogIndex
		failTerm := rf.log[nextIndex].Term
		for rf.log[nextIndex].Term == failTerm {
			nextIndex--
		}
		reply.NextIndex = nextIndex + 1
		return
	}
	var updateLogIndex int
	if len(args.Entry) != 0 {
		for i, entry := range args.Entry {
			curIndex := entry.Index
			if curIndex < len(rf.log) {
				if curIndex > rf.log[len(rf.log)-1].Index {
				}
				if entry.Term != rf.log[curIndex].Term {
					rf.log = append(rf.log[:curIndex], entry)
				}
			} else {
				rf.log = append(rf.log, args.Entry[i:]...)
				break
			}
		}
		reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
		updateLogIndex = reply.NextIndex - 1
	} else {
		reply.NextIndex = args.PrevLogIndex + 1
		updateLogIndex = args.PrevLogIndex
	}
	reply.Success = true
	rf.updateFollowCommit(args.LeaderCommit, updateLogIndex)
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(HEARTBEAT * time.Millisecond):
		ret = false
	}
	return ret
}
func (rf *Raft) doAppendEntries(server int) {
	for rf.role == Leader {
		rf.mu.Lock()
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		
		if rf.nextIndex[server] <  len(rf.log) {
			args.Entry = rf.log[rf.nextIndex[server]:]
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		if rf.sendAppendEntries(server, args, &reply) {
			rf.DoAppendEntriesReply(server, args, reply)
			return
		}
	}
}
func (rf *Raft) DoAppendEntriesReply(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if rf.role != Leader {
		return
	}
	if args.Term != rf.currentTerm || reply.Term > args.Term {
		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = Follower
			rf.chanRole <- true
			rf.persist()
			rf.mu.Unlock()
		}
		return
	}

	if reply.Success {
		rf.matchIndex[server] = reply.NextIndex - 1
		rf.nextIndex[server] = reply.NextIndex
	} else {
		rf.nextIndex[server] = reply.NextIndex
	}
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
	index := -1
	term := -1
	isLeader := false
	if rf.role == Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		entry := LogEntry{Command:command, Term:term, Index: index}
		rf.log = append(rf.log, entry)
	}
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
	rf.role = Follower
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.chanHeartbeat = make(chan bool)

	rf.chanRole = make(chan bool)
	rf.chanCommitted = applyCh
	rf.isKilled = false
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())

	go rf.changeRole()
	go rf.startElectTimer()

	return rf
}

func (rf *Raft) changeRole() {
	for !rf.isKilled {
		switch rf.role {
		case Leader:
			for i := range rf.peers {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
				rf.matchIndex[i] = 0
			}
			go rf.doHeartbeat()
			<-rf.chanRole
		case Candidate:
			go rf.startElection()
			<-rf.chanRole
		case Follower:
			<-rf.chanRole
		}
	}
}
func (rf *Raft) updateLeaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	oldIndex := rf.commitIndex
	newIndex := oldIndex
	for i := len(rf.log)-1; rf.log[i].Index>oldIndex && rf.log[i].Term==rf.currentTerm; i-- {
		countServer := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= rf.log[i].Index {
				countServer++
			}
		}
		if countServer * 2 > len(rf.peers) {
			newIndex = rf.log[i].Index
			break
		}
	}
	if oldIndex == newIndex {
		return
	}

	rf.commitIndex = newIndex
	for i := oldIndex + 1; i <= newIndex; i++ {
		rf.chanCommitted <- ApplyMsg{Index:i, Command:rf.log[i].Command}
		rf.lastApplied = i
	}
}
func (rf *Raft) updateFollowCommit(leaderCommit int, lastIndex int) {
	oldVal := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		if leaderCommit < lastIndex {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	for oldVal++; oldVal <= rf.commitIndex; oldVal++ {
		rf.chanCommitted <- ApplyMsg{Index:oldVal, Command:rf.log[oldVal].Command}
		rf.lastApplied = oldVal
	}
}
func (rf *Raft) doHeartbeat() {
	for index := range rf.peers {
		if index == rf.me {
			go func() {
				heartbeatTimer := time.NewTimer(HEARTBEAT * time.Millisecond)
				for rf.role == Leader {
					rf.chanHeartbeat <- true
					rf.updateLeaderCommit()
					heartbeatTimer.Reset(HEARTBEAT * time.Millisecond)
					<-heartbeatTimer.C
				}
			}()
		} else {
			go func(server int) {
				heartbeatTimer := time.NewTimer(HEARTBEAT * time.Millisecond)
				for rf.role == Leader {
					rf.doAppendEntries(server)
					heartbeatTimer.Reset(HEARTBEAT * time.Millisecond)
					<-heartbeatTimer.C
				}
			}(index)
		}
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	chanGather := make(chan bool, len(rf.peers))
	isOpen := true
	rf.currentTerm++
	rf.votedFor = rf.me
	chanGather <- true
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	lastLog := rf.log[len(rf.log)-1]
	args.LastLogIndex = lastLog.Index
	args.LastLogTerm = lastLog.Term
	rf.persist()
	rf.mu.Unlock()
	
	for index := range rf.peers {
		if rf.me != index {
			go func (index int) {
				if !isOpen {
					return
				}
				var reply RequestVoteReply
				if rf.sendRequestVote(index, args, &reply) {
					if args.Term < reply.Term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						rf.chanRole <- true
						rf.persist()
						isOpen = false
						rf.mu.Unlock()
						return
					} else if reply.VoteGranted && isOpen {
						chanGather <- true
					}
				} else if isOpen {
					chanGather <- false
				}
			}(index)
		}
	}

	yes, no := 0, 0
	isLoop := true
	for isLoop {
		select {
		case ok := <- chanGather:
			if ok {
				yes++
			} else {
				no++
			}
			if yes * 2 > len(rf.peers) {
				rf.role = Leader
				rf.chanRole <- true
				isLoop = false
			} else if no * 2 > len(rf.peers) {
				isLoop = false
			}
		}
		if isOpen == false{
			isLoop = false
		}
	}
	isOpen = false
	time.Sleep(10*time.Millisecond)
	close(chanGather)
}

func (rf *Raft) startElectTimer() {
	timer := time.NewTimer(10 * time.Millisecond)
	for !rf.isKilled{
		t := rand.Intn(ELETIMEMAX - ELETIMEMIN) + ELETIMEMIN
		timer.Reset(time.Duration(t) * time.Millisecond)
		select{
		case <- rf.chanHeartbeat:
		case <- timer.C:
			rf.role = Candidate
			rf.chanRole <- true
		}
	}
}