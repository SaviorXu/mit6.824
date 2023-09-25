package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"regexp"
	"fmt"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }
type WorkerAsk struct {
	Status   int //0表示请求任务，1表示完成map任务，2表示完成reduce任务
	FileName string
	TaskId   int
}

type CoordinatorReply struct {
	TaskType int //1表示map任务，2表示reduce任务,0表示无任务,3表示任务结束
	NReduce  int
	FileName string
	TaskId   int//任务序号
	ReduceId int//reduce文件所对应的下标
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

func GetFilesFromDir(fileName string) []string{
	files,err:=os.ReadDir(".")
	var fileList []string
	reg := regexp.MustCompile(fileName)
	if err!=nil{
		fmt.Println("RunReduce:read dir failed")
		return fileList
	}
	for _,file := range files{
		if reg.MatchString(file.Name()){
			fileList=append(fileList,file.Name())
		}
	}
	return fileList	
}