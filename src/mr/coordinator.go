package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Task struct{
	beginTime time.Time
	fileName string
}

type Coordinator struct {
	// Your definitions here.
	files map[string]int
	nReduce int
	mapTask []*Task
	reduceTask []*Task
	mutex sync.Mutex
	mapFinish int
	reduceFinish int
}

// Your code here -- RPC handlers for the worker to call.

func addReduce(c *Coordinator,reply *CoordinatorReply){

}

func addMap(c *Coordinator,reply *CoordinatorReply,fileName string){
	mapTask := &Task{time.Now(),fileName}
	c.mapTask=append(c.mapTask,mapTask)
	c.files[fileName]=0
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) getReq(args *WorkerAsk, reply *CoordinatorReply) error {
	if(args.status==0){
		//请求获取任务
	}else if(args.status==1){
		//map任务完成
		if(c.files[args.fileName]==0){
			c.files[args.fileName]=1
			c.mapFinish++
		}
	}else if(args.status==2){
		c.reduceFinish++
		//reduce任务完成
	}


	c.mutex.Lock()//不明白为什么要用锁
	count:=0
	if(c.mapFinish==1){
		addReduce(c,reply)
	}else{
		for key,value := range c.files{
			if(value==-1){//文件未分配
				//添加一个Map任务
				addMap(c,reply,key)
			}else if(value==1){//文件已完成
				count++
			}
		}
	}
	if(count==len(c.files)){
		c.mapFinish=1
	}
	c.mutex.Unlock()
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
	if(c.reduceFinish==c.nReduce){
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
	for _,value := range files{
		c.files[value]=-1//-1表示未分配，0表示已分配，1表示完成
	}

	c.nReduce=nReduce
	c.mapFinish=0
	c.reduceFinish=0

	c.server()
	return &c
}
