package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }
type WorkerAsk struct {
	status   int //0表示请求任务，1表示完成map任务，2表示完成reduce任务
	fileName string
	taskId   int
}

type CoordinatorReply struct {
	taskType int //0表示map任务，1表示reduce任务,-1表示无任务
	nreduce  int
	fileName string
	taskId   int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
