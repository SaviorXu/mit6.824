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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// 描述机器此时处于什么状态
const (
	stateFollower  = 0
	stateCandidate = 1
	stateLeader    = 2
)

// 描述机器的状态转换事件
const (
	heartBeat    = 0
	electTimeOut = 1
	electReady   = 2
)
const serverSum int = 100

// A Go object implementing a single Raft peer.
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
	votedFor    int //值为-1时，表示还未投票。否则，表示投给了谁
	//表示当前任期内选票投给了哪个候选者，持久化存储
	log []LogEntry //日志，持久化存储。一个日志被存储在超过半数的节点上，则认为该记录已提交，状态机可以安全地执行该记录。
	//
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	state          int //0:follower 1:candidate 2:leader

	commitIndex int
	lastApplied int
	nextIndex   []int //对于每个节点，下次发送日志的index
	matchIndex  []int //对于每个节点，已知的最后成功复制过去的index
	voteSum     int
	applyCh     chan ApplyMsg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// follower变成candidate，发起投票
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //任期号
	CandidateId  int //候选者id
	LastLogIndex int //上一个日志的索引号
	LastLogTerm  int //上一个日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// follower回复candidate。回复投票结果
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
	heartBeatTime := time.Millisecond * time.Duration(100)
	return heartBeatTime
}

func GetElectionTime() time.Duration {
	electTimeOut := rand.Intn(150) + 150
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
	term = rf.currentTerm
	isleader = false
	if rf.state == stateLeader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

func (rf *Raft) leaderCommit() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == stateLeader {
			leaderCommitIndex := 0
			leaderLastLogIndex := rf.log[len(rf.log)-1].Index
			for i := rf.commitIndex + 1; i <= leaderLastLogIndex; i++ {
				count := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.log[i].Index < rf.nextIndex[j] {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					leaderCommitIndex = rf.log[i].Index
				} else {
					break
				}
			}
			isThisCurrentTerm := false
			for i := leaderCommitIndex; i > rf.commitIndex; i-- {
				if rf.log[i].Term == rf.currentTerm {
					isThisCurrentTerm = true
					break
				}
			}
			if isThisCurrentTerm == true {
				//命令一定要按正确的顺序应用
				for i := rf.commitIndex + 1; i <= leaderCommitIndex; i++ {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: rf.log[i].Index,
					}
					rf.applyCh <- applyMsg
				}
				if leaderCommitIndex > rf.commitIndex {
					rf.commitIndex = leaderCommitIndex
					DPrintf("leader modify commitIndex=%v rf.me=%v rf.currentTerm=%v", rf.commitIndex, rf.me, rf.currentTerm)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := true
	rf.mu.Lock()
	if rf.state == stateLeader {
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		idx := rf.nextIndex[server] - 1
		args.PrevLogIndex = rf.log[idx].Index
		args.PrevLogTerm = rf.log[idx].Term
		args.Entries = make([]LogEntry, rf.getLastLogIndex()+1-rf.nextIndex[server])
		copy(args.Entries, rf.log[rf.nextIndex[server]:rf.getLastLogIndex()+1])
		rf.mu.Unlock()
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		DPrintf("sendAppendEntries reply1: rf.me=%v send to server=%v ok=%v reply.success=%v reply.Term=%v args.PreIdx=%v args.Term=%v args.commit=%v", rf.me, server, ok, reply.Success, reply.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = stateFollower
				rf.voteSum = 0
				rf.votedFor = -1
			}
			rf.mu.Unlock()
			//若发送失败，则会逐渐递减nextIndex。直到和server的prevTerm和prevIndex一致。
			var endLogIndex int
			if len(args.Entries) == 0 {
				endLogIndex = args.PrevLogIndex
			} else {
				endLogIndex = args.Entries[len(args.Entries)-1].Index
			}
			for ok == true && reply.Success == false {
				rf.mu.Lock()
				if rf.state != stateLeader {
					rf.mu.Unlock()
					break
				}
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server] -= 1
				}
				args.PrevLogIndex = rf.log[rf.nextIndex[server]-1].Index
				args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
				EntriesLen := endLogIndex - rf.nextIndex[server] + 1
				args.Entries = make([]LogEntry, EntriesLen)
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.LeaderId = rf.me
				copy(args.Entries, rf.log[rf.nextIndex[server]:endLogIndex+1])
				DPrintf("sendAppendEntries args2:args.LeaderId=%v server=%v args.LeaderCommit=%v args.PrevLogIndex=%v args.PrevLogTerm=%v len(logEntry)=%v", args.LeaderId, server, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
				rf.mu.Unlock()
				ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
				rf.mu.Lock()
				DPrintf("sendAppendEntries reply2: rf.me=%v send to server=%v ok=%v reply.success=%v reply.Term=%v args.PreIdx=%v args.Term=%v args.commit=%v", rf.me, server, ok, reply.Success, reply.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
				//必须检测ok是否为true，否则有可能是上次的reply。因为reply变量没有重新创建
				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = stateFollower
						rf.voteSum = 0
						rf.votedFor = -1
					}
				}
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			if reply.Success && rf.state == stateLeader {
				rf.nextIndex[server] = endLogIndex + 1
				rf.matchIndex[server] = endLogIndex
			}
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}

	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := true
	if server == rf.me {
		rf.mu.Lock()
		rf.electionTimer.Reset(GetElectionTime())
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		//当A,B,C三个机器。A先超时，B给A投票，A当选leader后，有一个request，之后C超时，当选leader，A的request就不能达到一致。
		//原因是A当选为leader，就不会给C发送投票，如果rf.state=stateCandidate。C就不会重新设置超时选举定时器
		if rf.state != stateFollower {
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.getLastLogTerm()
			rf.mu.Unlock()
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = stateFollower
					rf.voteSum = 0
					rf.votedFor = -1
				} else {
					if rf.state == stateCandidate {
						if reply.VoteGranted {
							rf.voteSum += 1
						}
						if rf.voteSum > len(rf.peers)/2 {
							rf.state = stateLeader
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							DPrintf("%v become leader term=%v\n", rf.me, rf.currentTerm)
						}
					}
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
	}
	return ok
}

// example RequestVote RPC handler.
// 跟随者根据args决定是否投票给该候选者。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.voteSum = 0
		rf.votedFor = -1
	}

	DPrintf("RequestVote candidate:[id=%v,term=%v,Lastindex=%v,Lastterm=%v] my[id=%v,term=%v,voteFor=%v,lastindex=%v,lastterm=%v]\n", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.currentTerm, rf.votedFor, rf.getLastLogIndex(), rf.getLastLogTerm())
	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		//任期比自己小 ， 该任期票已投
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.votedFor == -1 {
		lastIdx := rf.getLastLogIndex()
		lastTerm := rf.getLastLogTerm()
		if args.LastLogTerm < rf.log[lastIdx].Term || (args.LastLogTerm == lastTerm && args.LastLogIndex < rf.log[lastIdx].Index) {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.electionTimer.Reset(GetElectionTime())
		}
	}
	DPrintf("RequestVote finish candidate:[id=%v,term=%v,Lastindex=%v,Lastterm=%v] my[id=%v,term=%v,voteFor=%v,lastindex=%v,lastterm=%v,rf.voteGranted=%v]\n", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.currentTerm, rf.votedFor, rf.getLastLogIndex(), rf.getLastLogTerm(), reply.VoteGranted)
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.voteSum = 0
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		DPrintf("AppendEntries apply fail1 args.leaderId=%v args.term=%v args.PrevLogIndex=%v args.PrevLogTerm=%v myid=%v,myterm=%v,lastIdx=%v,lastTerm=%v\n", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
		rf.mu.Unlock()
		return
	}
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		DPrintf("AppendEntries apply fail2 args.leaderId=%v args.term=%v args.PrevLogIndex=%v args.PrevLogTerm=%v myid=%v,myterm=%v,lastIdx=%v,lastTerm=%v\n", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
		rf.mu.Unlock()
		return
	}

	//刚好一致 或 在领导者的最后一个索引处，任期一致。则删除这之后多余的
	//当args.Entries=0时，就不会检查日志是否一致
	argsPrevIndex := args.PrevLogIndex
	for key, entry := range args.Entries {
		argsPrevIndex++
		if argsPrevIndex < len(rf.log) {
			if rf.log[argsPrevIndex].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:argsPrevIndex]
		}
		rf.log = append(rf.log, args.Entries[key:]...)
		break
	}
	reply.Success = true
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	rf.electionTimer.Reset(GetElectionTime())

	// rf.log = rf.log[:args.PrevLogIndex+1]
	// rf.log = append(rf.log, args.Entries...)
	// reply.Success = true
	// reply.Term = rf.currentTerm
	// rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	// rf.electionTimer.Reset(GetElectionTime())

	DPrintf("AppendEntries apply args.LeaderCommit=%v rf.commitIndex=%v\n", args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		var tmpLastIndex int
		if args.LeaderCommit > rf.getLastLogIndex() {
			tmpLastIndex = rf.getLastLogIndex()
		} else {
			tmpLastIndex = args.LeaderCommit
		}
		for j := rf.commitIndex + 1; j <= tmpLastIndex; j++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[j].Command,
				CommandIndex: rf.log[j].Index,
			}
			DPrintf("AppendEntries apply j=%v\n", j)
			rf.applyCh <- applyMsg
		}
		rf.commitIndex = tmpLastIndex
	}
	DPrintf("AppendEntries apply finish args.leaderId=%v args.term=%v args.PrevLogIndex=%v args.PrevLogTerm=%v myid=%v,myterm=%v,lastIdx=%v,lastTerm=%v,len(log)=%v\n", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm(), len(rf.log))
	rf.mu.Unlock()
}

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
// 第一个返回值表示这个请求将会存放在log中的位置，第二个表示任期号，第三个表示是否是领导者
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := false

	rf.mu.Lock()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	if rf.state == stateLeader {
		isLeader = true
		logEntry := LogEntry{index, term, command}
		rf.log = append(rf.log, logEntry)
		rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
		DPrintf("Start rf.me=%v rf.state=%v cmd=%v len(rf.log)=%v\n", rf.me, rf.state, command, len(rf.log))
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1) //将rf.dead的值设置为1
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) //返回rf.dead的值
	return z == 1
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) stateMachine(state int) {
	rf.mu.Lock()
	DPrintf("stateMachine state=%v rf.me=%v rf.currentTerm=%v rf.state=%v len(rf.log)=%v rf.commitIndex=%v", state, rf.me, rf.currentTerm, rf.state, len(rf.log), rf.commitIndex)
	DPrintf("stateMachine rf.lastIndex=%v rf.lastTerm=%v cmd=%v", rf.getLastLogIndex(), rf.getLastLogTerm(), rf.log[rf.getLastLogIndex()].Command)
	// for i := 0; i < len(rf.log); i++ {
	// 	DPrintf("stateMachine rf.Index=%v rf.Term=%v rf.Command=%v", rf.log[i].Index, rf.log[i].Term, rf.log[i].Command)
	// }
	rf.mu.Unlock()
	rf.mu.Lock()
	rfState := rf.state
	rf.mu.Unlock()
	switch rfState {
	case stateFollower:
		if state == electTimeOut {
			rf.mu.Lock()
			DPrintf("stateMachine state=electTimeOut :[Peer%v Term%v state%v]", rf.me, rf.currentTerm, rf.state)
			rf.currentTerm += 1
			rf.state = stateCandidate
			rf.votedFor = rf.me
			rf.voteSum = 1
			rf.mu.Unlock()
			rf.stateMachine(electReady)
		}
	case stateCandidate:
		if state == electReady {
			for i := 0; i < len(rf.peers); i++ {
				var args RequestVoteArgs
				var reply RequestVoteReply
				go rf.sendRequestVote(i, &args, &reply)
			}
		} else if state == electTimeOut {
			rf.mu.Lock()
			DPrintf("stateMachine state=electTimeOut :[Peer%v Term%v state%v]", rf.me, rf.currentTerm, rf.state)
			rf.currentTerm += 1
			rf.state = stateCandidate
			rf.votedFor = rf.me
			rf.voteSum = 1
			rf.mu.Unlock()
			rf.stateMachine(electReady)
		}
	case stateLeader:
		if state == heartBeat {
			rf.mu.Lock()
			DPrintf("stateMachine state=heartBeat :[Peer%v Term%v state%v]", rf.me, rf.currentTerm, rf.state)
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					var args AppendEntriesArgs
					var reply AppendEntriesReply
					go rf.sendAppendEntries(i, &args, &reply)
				}
			}
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// ？？?leader在超时选举时间内没有收到任何响应，如何处理（不可能出现该情况..只有领导者才会发送心跳包，当领导者发送心跳包，他就会收到响应）
// 选举超时时间：一个随机数在150-300ms。当follower在选举超时时间内没有收到leader的响应，则申请当leader，状态变为candidate。
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			rf.heartBeatTimer.Reset(GetHeartBeatTime())
			rf.mu.Unlock()
			rf.stateMachine(heartBeat)
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.electionTimer.Reset(GetElectionTime())
			rf.mu.Unlock()
			rf.stateMachine(electTimeOut)
		}
	}
}

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
	DPrintf("Raft start...")
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		heartBeatTimer: time.NewTimer(GetHeartBeatTime()),
		electionTimer:  time.NewTimer(GetElectionTime()),
		state:          stateFollower,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		voteSum:        0,
		applyCh:        applyCh,
	}
	rf.log = make([]LogEntry, 1)
	rf.log[0].Command = nil
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	DPrintf("raft term is %v", rf.currentTerm)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderCommit()

	return rf
}
