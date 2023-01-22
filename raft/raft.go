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
	"labrpc"
	"math/rand"
	"sync"
	"time"
	// "fmt"
)

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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

	electionStarted bool

	term             int
	state            string
	isLeader         bool
	votedFor         int
	voteCount        int
	voteTrack        int
	timeoutDuration  time.Duration
	heartBeatTimeout time.Duration
	lastAppendEntry  time.Time
	ticker           *time.Ticker
	heartBeatTicker  *time.Ticker

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	exitTimeout chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	t := rf.term
	l := rf.isLeader
	rf.mu.Unlock()
	return t, l
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
	e.Encode(rf.term)
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
	rf.mu.Lock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	rf.electionStarted = true
	rf.ticker = time.NewTicker(rf.timeoutDuration)
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	} else {
		reply.Term = args.Term
		if rf.term < args.Term {
			rf.term = args.Term
			rf.state = "follower"
			rf.isLeader = false
			rf.votedFor = -1
		}
		// checking at least as uptodate as my log
		isUptoDate := false
		if (args.LastLogTerm != rf.GetLogLastTerm() && args.LastLogTerm > rf.GetLogLastTerm()) || (args.LastLogTerm == rf.GetLogLastTerm() && args.LastLogIndex >= rf.GetLogLastIndex()) {
			isUptoDate = true
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUptoDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
		rf.persist()
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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

	rf.mu.Lock()

	if rf.isLeader {
		index = rf.GetLogLastIndex() + 1
		term = rf.term
		isLeader = true
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.persist()
	}

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

	rf.electionStarted = false

	// Your initialization code here.
	rf.term = 0
	rf.state = "follower"
	rf.isLeader = false
	rf.votedFor = -1
	rf.voteCount = 0
	rf.voteTrack = 0

	// Initial Entry - Ignore this index
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.timeoutDuration = time.Duration(rand.Intn(600-300)+300) * time.Millisecond
	rf.heartBeatTimeout = 50 * time.Millisecond
	rf.ticker = time.NewTicker(rf.timeoutDuration)
	rf.heartBeatTicker = time.NewTicker(rf.heartBeatTimeout)
	rf.lastAppendEntry = time.Now()

	rf.exitTimeout = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// timeout checker
	go rf.CheckTimeOut()

	return rf
}

type AppendEntryStruct struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReplyStruct struct {
	Term              int
	Success           bool
	ConflictEntryData ConflictEntryStruct
}

type ConflictEntryStruct struct {
	Index int
	Term  int
}

func (rf *Raft) GetLogLastIndex() int {
	l := len(rf.log)
	return rf.log[l-1].Index
}

func (rf *Raft) GetLogLastTerm() int {
	l := len(rf.log)
	return rf.log[l-1].Term
}

func (rf *Raft) ApplyChannelRoutine() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{Index: rf.log[rf.lastApplied].Index, Command: rf.log[rf.lastApplied].Command}
		rf.applyCh <- msg
	}
}

func (rf *Raft) ReceiveAppendEntry(args AppendEntryStruct, reply *AppendEntryReplyStruct) {
	rf.mu.Lock()
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		reply.Term = args.Term
		if rf.term < args.Term {
			rf.term = args.Term
			rf.persist()
		}
		rf.isLeader = false

		if args.PrevLogIndex > rf.GetLogLastIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Success = false

			// if leaders log is bigger than me
			if args.PrevLogIndex <= rf.GetLogLastIndex() {
				index := args.PrevLogIndex
				for i := 0; i < len(rf.log); i++ {
					for rf.log[index].Term == rf.log[args.PrevLogIndex].Term && index > 0 {
						index--
					}
				}
				reply.ConflictEntryData = ConflictEntryStruct{index + 1, rf.log[args.PrevLogIndex].Term}
			} else {
				reply.ConflictEntryData = ConflictEntryStruct{len(rf.log), 0}
			}

			// updating append entry time
			rf.lastAppendEntry = time.Now()
			rf.ticker = time.NewTicker(rf.timeoutDuration)
			rf.mu.Unlock()
			return
		} else {
			reply.Success = true

			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries[:]...)

			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < rf.GetLogLastIndex() {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = rf.GetLogLastIndex()
				}
				rf.ApplyChannelRoutine()
			}
			// updating append entry time
			rf.lastAppendEntry = time.Now()
			rf.ticker = time.NewTicker(rf.timeoutDuration)
			rf.persist()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryStruct, reply *AppendEntryReplyStruct) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntries() {
	for {
		rf.mu.Lock()
		t := rf.heartBeatTimeout
		rf.mu.Unlock()
		time.Sleep(t)
		rf.mu.Lock()
		if rf.isLeader {
			if len(rf.peers) == 1 {
				rf.state = "follower"
				rf.isLeader = false
				rf.persist()
			}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					prevLogIndex := rf.nextIndex[i] - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					args := AppendEntryStruct{rf.term, rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[i]:], rf.commitIndex}
					reply := new(AppendEntryReplyStruct)
					go rf.AppendEntryGoRoutine(i, args, reply)
				}
			}
		} else {
			break
		}
		rf.mu.Unlock()
		// }
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntryGoRoutine(server int, args AppendEntryStruct, reply *AppendEntryReplyStruct) {
	if rf.sendAppendEntry(server, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.term && !reply.Success {
			rf.term = reply.Term
			rf.state = "follower"
			rf.isLeader = false
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			for i := rf.GetLogLastIndex(); i >= rf.commitIndex; i-- {
				// 1 is because we are counting ourselve by default
				count := 1
				for j := 0; j < len(rf.matchIndex); j++ {
					if j != rf.me && rf.matchIndex[j] >= i {
						count++
					}
					if count > len(rf.matchIndex)/2 && rf.log[i].Term == rf.term {
						rf.commitIndex = i
						rf.ApplyChannelRoutine()
						break
					}
				}
			}
		} else {
			rf.nextIndex[server] = rf.nextIndex[server] - reply.ConflictEntryData.Index
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CheckTimeOut() {
	for {
		rf.mu.Lock()
		t0 := rf.timeoutDuration
		rf.mu.Unlock()
		time.Sleep(t0)
		rf.mu.Lock()
		t := time.Now()
		if rf.state == "follower" {
			diff := t.Sub(rf.lastAppendEntry) * time.Millisecond
			if time.Duration(diff) > rf.timeoutDuration {
				go rf.startElection()
			}
		} else if rf.state == "candidate" {
			// election timeout . re election
			go rf.startElection()
		}
		rf.mu.Unlock()
		// }
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = "candidate"
	if rf.electionStarted {
		rf.term++
	} else {
		rf.electionStarted = true
	}
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.voteTrack = 1
	// rf.ticker.Stop()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := RequestVoteArgs{rf.term, rf.me, rf.GetLogLastIndex(), rf.GetLogLastTerm()}
			reply := new(RequestVoteReply)
			go rf.RequestVoteGoRoutine(i, args, reply)
		}
	}
	rf.persist()
	// rf.ticker = time.NewTicker(rf.timeoutDuration)
	rf.mu.Unlock()
}

func (rf *Raft) RequestVoteGoRoutine(server int, args RequestVoteArgs, reply *RequestVoteReply) {
	if rf.sendRequestVote(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.state = "follower"
			rf.persist()
			return
		}
		if rf.state != "candidate" || rf.term != args.Term {
			return
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				rf.state = "leader"
				rf.isLeader = true

				// initializing nextIndices and matchIndices
				rf.nextIndex = make([]int, 0)
				rf.matchIndex = make([]int, 0)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex = append(rf.nextIndex, rf.GetLogLastIndex()+1)
					rf.matchIndex = append(rf.matchIndex, 0)
				}

				go rf.AppendEntries()
			}
		}
		// rf.voteTrack++
	}
}
