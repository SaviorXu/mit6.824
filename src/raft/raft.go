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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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

type LogEntry struct {
	Index   int
	Term    int //日志条目首次被领导者创建时的任期
	Command interface{}
}

const (
	stateFollower  = 0
	stateCandidate = 1
	stateLeader    = 2
)
const serverSum int = 100

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int //服务器当前已知的最新任期号，必须持久化存储。
	votedFor    int //值为0时，表示还未投票。值为1时，表示已投票
	//表示当前任期内选票投给了哪个候选者，持久化存储
	log []LogEntry //日志，持久化存储。一个日志被存储在超过半数的节点上，则认为该记录已提交，状态机可以安全地执行该记录。
	//
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	state          int //0:follower 1:candidate 2:leader

	commitIndex int
	lastApplied int
	nextIndex   []int //对于每个节点，下次发送日志的index
	matchIndex  []int
	grantVote   chan int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//follower变成candidate，发起投票
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //任期号
	CandidateId  int //候选者id
	LastLogIndex int //上一个日志的索引号
	LastLogTerm  int //上一个日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//follower回复candidate。回复投票结果
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //任期号
	VoteGranted bool //是否投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int //代表领导者已提交的最大的日志索引。跟随者将提交日志索引小于LeaderCommit的日志，并将该日志中的命令应用到自己的状态机。
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func GetHeartBeatTime() time.Duration {
	heartBeatTimeOut := rand.Intn(20) + 1
	heartBeat := time.Millisecond * time.Duration(heartBeatTimeOut)
	return heartBeat
}

func GetElectionTime() time.Duration {
	electTimeOut := rand.Intn(151) + 150
	elecTime := time.Millisecond * time.Duration(electTimeOut)
	return elecTime
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
	isleader = false
	if rf.state == 2 {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
// example RequestVote RPC handler.
//跟随者根据args决定是否投票给该候选者。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(GetElectionTime())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.votedFor = 0
	}
	if args.Term < rf.currentTerm || rf.votedFor == 1 {
		//任期比自己小 ， 该任期票已投
		fmt.Println("Not Vote")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.votedFor == 0 {
		lastIdx := rf.getLastLogIndex()
		lastTerm := rf.getLastLogTerm()
		if lastIdx == -1 && lastTerm == -1 {
			fmt.Println(rf.me, " Vote")
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = 1
		} else if args.LastLogTerm < rf.log[lastIdx].Term || (args.LastLogTerm == rf.log[lastIdx].Term && args.LastLogIndex < rf.log[lastIdx].Index) {
			fmt.Println("Not Vote")
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			fmt.Println(rf.me, " Vote")
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = 1
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(GetElectionTime())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.votedFor = 0
	}
	if args.Term < rf.currentTerm || len(args.Entries) == 0 {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		//还需要修改commitIndex
		if args.PrevLogIndex == rf.getLastLogIndex() && args.PrevLogTerm == rf.getLastLogTerm() ||
			args.PrevLogIndex <= rf.getLastLogIndex() && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
			//刚好一致 或 在领导者的最后一个索引处，任期一致。则删除这之后多余的
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			reply.Term = rf.currentTerm

			if args.LeaderCommit > rf.commitIndex {
				if rf.getLastLogIndex() > rf.commitIndex {
					rf.commitIndex = rf.getLastLogIndex()
				}
			}
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
	}
}

//如果跟随者宕机或请求超时，日志没有复制成功，领导者将反复尝试发送AppendEntries消息。
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
//领导者向所有的跟随者发起投票。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
//第一个返回值表示的日志的索引号，第二个表示任期号，第三个表示是否是领导者
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := false

	rf.mu.Lock()
	index := len(rf.log) - 1
	term := rf.currentTerm
	if rf.state == stateLeader {
		isLeader = true
		logEntry := LogEntry{index, term, command}
		rf.log = append(rf.log, logEntry)
		index = len(rf.log) - 1

		entryPackage := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.getLastLogIndex(),
			PrevLogTerm:  rf.getLastLogTerm(),
			LeaderCommit: rf.commitIndex,
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				entryPackage.Entries = rf.log[rf.nextIndex[i]:]
				var entryReply AppendEntriesReply
				rf.sendAppendEntries(i, &entryPackage, &entryReply)
				//处理跟随者的回复
				if entryReply.Success == true {
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
				} else {
					//处理循环发送的问题
				}
			}

		}
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

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
	atomic.StoreInt32(&rf.dead, 1) //将rf.dead的值设置为1
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) //返回rf.dead的值
	return z == 1
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	} else {
		return rf.log[len(rf.log)-1].Index
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) broadCastHeartBeat() {
	heartBeatPackage := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm(),
		LeaderCommit: rf.commitIndex,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var heartBeatReply AppendEntriesReply
			heartBeatReply.Success = false
			rf.sendAppendEntries(i, &heartBeatPackage, &heartBeatReply)
			//收到该心跳的返回包之后的处理
		}
	}
}

func (rf *Raft) startElect() {
	voteSum := 1
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	var requestVoteReply RequestVoteReply
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == stateCandidate {
			go func() {
				ok := rf.peers[i].Call("Raft.RequestVote", requestVoteArgs, requestVoteReply)
				if ok == true {
					if requestVoteReply.Term > rf.currentTerm {
						rf.currentTerm = requestVoteReply.Term
						rf.state = stateFollower
						rf.votedFor = 0
					}
					if requestVoteReply.VoteGranted {
						voteSum += 1
					}
				}
			}()
		}
	}
	if rf.state == stateCandidate && voteSum >= len(rf.peers)/2 {
		rf.state = stateLeader
		fmt.Println(rf.me, " become leader")
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//？？?leader在超时选举时间内没有收到任何响应，如何处理（不可能出现该情况..只有领导者才会发送心跳包，当领导者发送心跳包，他就会收到响应）
//选举超时时间：一个随机数在150-300ms。当follower在选举超时时间内没有收到leader的响应，则申请当leader，状态变为candidate。
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == stateLeader {
				rf.broadCastHeartBeat()
			}
			rf.mu.Unlock()
			fmt.Println("heartBeatTimer timeout")
		case <-rf.electionTimer.C:
			fmt.Println("electionTimer timeout")
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.state = stateCandidate
			rf.votedFor = 1
			rf.startElect()
			rf.mu.Unlock()
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Raft start...")
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       0,
		heartBeatTimer: time.NewTimer(GetHeartBeatTime()),
		electionTimer:  time.NewTimer(GetElectionTime()),
		state:          stateFollower,
		commitIndex:    -1,
		lastApplied:    -1,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		grantVote:      make(chan int, serverSum),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
