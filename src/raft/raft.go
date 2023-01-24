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

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
	//"fmt"
	"math/rand"
	
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type logEntry struct {
	term int
	entry interface{}
}

type Raft struct {

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ApplyCh chan ApplyMsg
	//** Persistent States
	currentTerm int
	votedFor int
	logs map[int]logEntry
	logLen int
	// states that I find necessary
	leaderIndex int
	isLeader bool
	needNewLeader bool
	stepDown bool

	//** Volatile States on All Servers

	//index of highest log entry known to be committed 
	commitIndex int
	//index of highest log entry applied to state machine
	lastApplied int

	//** Volatile state on leaders:
	nextIndex map[int]int
	matchIndex map[int]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if(rf.currentTerm > args.Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//fmt.Println("refuse vote ", args.CandidateId, "term old")
	} else {
		if(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.VoteGranted = true
		} else {
			//fmt.Println(rf.me, "refuse vote ", args.CandidateId, "votedFor", rf.votedFor)
		}
	}
	rf.mu.Unlock()
	return
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
//********************************************************************
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if(args.Term < rf.currentTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
	}
	
	//fmt.Println(args.PrevLogIndex,args.PrevLogTerm)
	reply.Success = true
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if(args.PrevLogIndex != 0) {
		entry, ok := rf.logs[args.PrevLogIndex]
		if(ok && entry.term != args.PrevLogTerm) || !ok {
			reply.Success = false
			return
		}
	}
	if(args.LeaderCommit > rf.lastApplied) {
		rf.commitIndex = args.LeaderCommit
		rf.lastApplied = rf.commitIndex
		rf.ApplyCh <- ApplyMsg{Command: rf.logs[rf.commitIndex].entry, CommandValid: true, CommandIndex : rf.commitIndex}
	}
	if(len(args.Entries) == 0) {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.isLeader = args.LeaderId == rf.me
		rf.needNewLeader = false
		rf.votedFor = args.LeaderId
		return
	}
	if(len(args.Entries) != 0) {
		reply.Term = args.Term
		for _, entry := range args.Entries {
			rf.logLen += 1
			rf.logs[rf.logLen] = logEntry{term: reply.Term, entry: entry}
		}
		rf.commitIndex = args.LeaderCommit
		rf.lastApplied = rf.commitIndex
		return
	}

}


//***********************************************************
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()

	index = rf.logLen + 1
	isLeader = rf.isLeader
	term = rf.currentTerm
	if(rf.isLeader && !rf.killed()) {
		go rf.sendLog(command)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}
//******************************************************************
//Send Log
func (rf *Raft) sendLog(command interface{}) {	
	mu := sync.Mutex{}
	agree := 1
	rf.mu.Lock()
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.logLen, PrevLogTerm: rf.logs[rf.logLen].term, Entries: []interface{}{command}, LeaderCommit: rf.commitIndex}
	rf.logLen += 1
	rf.logs[rf.logLen] = logEntry{term: rf.currentTerm, entry: command}
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		rf.mu.Lock()
		if(rf.me == server) {
			//fmt.Println("jump", server)
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go func (serverIndex int){
			reply := &AppendEntriesReply{}
			ok := rf.peers[serverIndex].Call("Raft.AppendEntries", args, reply)
			if !ok {
				//fmt.Println("error when sending heartBeat")
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !reply.Success {
					if(reply.Term > rf.currentTerm) {
						rf.stepDownFromLeader(reply.Term)
					}
				} else {
					mu.Lock()
					defer mu.Unlock()
					agree += 1
					if(agree >= (len(rf.peers) / 2 + 1)) {
						agree = 0
						rf.commitIndex += len(args.Entries)
						rf.lastApplied = rf.commitIndex
						rf.ApplyCh <- ApplyMsg{Command: command, CommandValid: true, CommandIndex : rf.commitIndex}
					}
				}
			}
		}(server)
	}
}
// Steps down from being a leader. must be called with rf.mu held !!!!!!!!
func (rf *Raft) stepDownFromLeader(term int) {
	rf.currentTerm = term
	rf.leaderIndex = -1
	rf.isLeader = false
	rf.stepDown = true
}

//******************************************************************
//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		randomTimeOut := getRandomTimeOut()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		rf.needNewLeader = true
		rf.mu.Unlock()
		time.Sleep(time.Duration(randomTimeOut) * time.Millisecond)
		term, _ := rf.GetState()
		rf.mu.Lock()
		//fmt.Println(rf.me,rf.needNewLeader)
		if(rf.needNewLeader) {
			rf.currentTerm = term + 1
			rf.votedFor = -1
			go rf.startElection(term + 1)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection(term int) {
	lastLogTerm := 1
	rf.mu.Lock()
	if(rf.logLen != 0) {
		lastLogTerm = rf.logs[rf.logLen].term
	}
	rf.mu.Unlock()
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	voteReceived := 0
	done := 0
	args := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: rf.logLen, LastLogTerm: lastLogTerm}
	for server, _ := range rf.peers {
		go func (serverIndex int, term int){
			reply := &RequestVoteReply{}
			ok := rf.peers[serverIndex].Call("Raft.RequestVote", args, reply)
			if !ok {
				//fmt.Println("error when sending request")
				cond.L.Lock()
				done += 1
				cond.L.Unlock()
				cond.Broadcast()
			} else {
				cond.L.Lock()
				done += 1
				if(reply.Term > term) {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
				}
				if !reply.VoteGranted {
					//fmt.Println("vote denied")
				}
				if reply.VoteGranted {
					voteReceived += 1
				}
				cond.L.Unlock()
				cond.Broadcast()
			}
		}(server, term)
	}
	cond.L.Lock()
	for (done != len(rf.peers) && voteReceived < (len(rf.peers) / 2 + 1)) {
		cond.Wait()
		rf.mu.Lock()
		if(voteReceived >= (len(rf.peers) / 2 + 1)) {
			if(rf.currentTerm == term) {
				//fmt.Println(rf.me, "is new leader for term", term)
				rf.leaderIndex = rf.me
				rf.isLeader = true
				rf.nextIndex = make(map[int]int)
				rf.matchIndex = make(map[int]int)
				for peer,_ := range rf.peers {
					rf.nextIndex[peer] = rf.logLen + 1
					rf.matchIndex[peer] = 0
				}
				go rf.sendHeartBeat()
				rf.needNewLeader = false
			} else {
				rf.votedFor = -1
			}
		} else {
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}
	cond.L.Unlock()
}

func (rf *Raft) appendOne(server int, entries interface{}) {
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.logLen, PrevLogTerm: rf.logs[rf.logLen].term, Entries: []interface{}{}, LeaderCommit: rf.commitIndex}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//fmt.Println("error when sending heartBeat")
	} else {
		if !reply.Success {
			//fmt.Println("heartbeat term old")
			rf.mu.Lock()
			if(reply.Term > rf.currentTerm) {
				rf.stepDownFromLeader(reply.Term)
			} else {
				rf.matchIndex[server] -= 1
			}
			rf.mu.Unlock()
		}
	}
}


func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	rf.stepDown = false
	rf.mu.Unlock()
	mu := sync.Mutex{}
	for rf.killed() == false {
		rf.mu.Lock()
		stepDown := rf.stepDown
		rf.mu.Unlock()
		mu.Lock()
		if(stepDown) {
			mu.Unlock()
			return
		}
		mu.Unlock()
		_, isleader := rf.GetState()
		if(isleader) {
			for server, _ := range rf.peers {
				rf.mu.Lock()
					//fmt.Println(rf.logs[rf.logLen].term,"dsfds",rf.logs[rf.logLen])
				rf.mu.Unlock()
				go rf.appendOne (server, []interface{}{})
			}
		}
		time.Sleep(120 * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.currentTerm = 1
	rf.logLen = 0
	rf.logs = make(map[int]logEntry)
	rf.votedFor = -1
	rf.isLeader = false
	rf.ApplyCh = applyCh
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for server,_ := range rf.peers {
		rf.nextIndex[server] = rf.logLen + 1
		rf.matchIndex[server] = 0
	} 
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
func getRandomTimeOut() int {
	rand.Seed(time.Now().UnixNano())
	min := 300
	max := 600
	return rand.Intn(max - min + 1) + min
}
