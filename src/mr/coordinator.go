package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	beginTime time.Time
	fileName  string
	taskId    int
}

type Coordinator struct {
	// Your definitions here.
	files        map[string]int
	nReduce      int
	mapTask      []*Task
	reduceTask   []*Task
	mutex        sync.Mutex
	mapIdx       int //map任务序号
	reduceIdx    int //reduce任务序号
	mapFinish    int //map任务完成数目
	reduceFinish int //reduce任务完成数目
}

// Your code here -- RPC handlers for the worker to call.

func addReduce(c *Coordinator, reply *CoordinatorReply) {
	if c.reduceIdx < c.nReduce {
		reply.taskType = 1
		reply.taskId = c.reduceIdx

		reduceTask := &Task{time.Now(), "", c.reduceIdx}
		c.reduceTask = append(c.reduceTask, reduceTask)
		c.reduceIdx++
	}
}

func addMap(c *Coordinator, reply *CoordinatorReply) {
	for fileName, status := range c.files {
		if status == -1 {
			reply.taskType = 0
			reply.nreduce = c.nReduce
			reply.taskId = c.mapIdx

			mapTask := &Task{time.Now(), fileName, c.mapIdx}
			c.mapTask = append(c.mapTask, mapTask)
			c.mapIdx++
			c.files[fileName] = 0
			break
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) getReq(args *WorkerAsk, reply *CoordinatorReply) error {
	if args.status == 0 {
		//请求获取任务,当map的任务完成时，分配reduce任务
		if c.mapFinish != len(c.files) {
			addMap(c, reply)
		} else {
			addReduce(c, reply)
		}
	} else if args.status == 1 {
		//map任务完成
		if c.files[args.fileName] == 0 {
			//从map的任务队列中删除该任务
			for key, value := range c.mapTask {
				if value.taskId == args.taskId {
					c.mapTask = append(c.mapTask[:key], c.mapTask[key+1:]...)
					c.files[args.fileName] = 1
					c.mapFinish++
					break
				}
			}
		}
		reply.taskType = -1
	} else if args.status == 2 {
		//reduce任务完成
		for key, value := range c.reduceTask {
			if value.taskId == args.taskId {
				c.reduceTask = append(c.reduceTask[:key], c.reduceTask[key+1:]...)
				c.reduceFinish++
				break
			}
		}
		reply.taskType = -1
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.reduceFinish == c.nReduce {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, value := range files {
		c.files[value] = -1 //-1表示未分配，0表示已分配，1表示完成
	}

	c.nReduce = nReduce
	c.mapFinish = 0
	c.reduceFinish = 0
	c.mapIdx = 0
	c.reduceIdx = 0

	c.server()
	return &c
}
