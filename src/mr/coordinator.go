package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"strconv"
	"regexp"
	"io/ioutil"
	"fmt"
)

type Task struct {
	BeginTime time.Time
	FileName  string
	TaskId    int//reduce or map任务号
	ReduceId int//reduce任务所对应的文件号
}

type Coordinator struct {
	// Your definitions here.
	MapFiles        map[string]int
	ReduceFiles []int	
	NReduce      int
	MapTask      []*Task
	ReduceTask   []*Task
	Mutex        sync.Mutex
	MapIdx       int //map任务序号
	ReduceIdx    int //reduce任务序号
	MapFinish    int //map任务完成数目
	ReduceFinish int //reduce任务完成数目
	Finish bool
}

// Your code here -- RPC handlers for the worker to call.
//coordinator还需要隔一段时间检查任务完成是否超时，10s内未完成将其给另外的worker
//还有锁没有完成

func addReduce(c *Coordinator, reply *CoordinatorReply) {
	for i:=0;i<c.NReduce;i++{
		if c.ReduceFiles[i]==-1{
			// fmt.Println("addReduce")
			reply.TaskType=2
			reply.TaskId=c.ReduceIdx
			reply.NReduce=c.NReduce	
			reply.ReduceId=i
			reduceTask:= &Task{time.Now(),"",c.ReduceIdx,i}
			c.ReduceFiles[i]=0
			c.ReduceTask=append(c.ReduceTask,reduceTask)
			c.ReduceIdx++
			break
		}
	}
}

func addMap(c *Coordinator, reply *CoordinatorReply) {
	for fileName, status := range c.MapFiles {
		if status == -1 {
			reply.TaskType = 1
			reply.NReduce = c.NReduce
			reply.TaskId = c.MapIdx
			reply.FileName=fileName

			mapTask := &Task{time.Now(), fileName, c.MapIdx,-1}
			c.MapTask = append(c.MapTask, mapTask)
			c.MapIdx++
			c.MapFiles[fileName] = 0
			break
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetReq(args *WorkerAsk, reply *CoordinatorReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.Status == 0 {
		//请求获取任务,当map的任务完成时，分配reduce任务
		if c.MapFinish < len(c.MapFiles) {
			addMap(c, reply)
		} else {
			addReduce(c, reply)
		}
	} else if args.Status == 1 {
		//map任务完成
		if c.MapFiles[args.FileName] == 0 {
			//从map的任务队列中删除该任务
			for key, value := range c.MapTask {
				if value.TaskId == args.TaskId {
					if(len(c.MapTask)>1){
						c.MapTask = append(c.MapTask[:key], c.MapTask[key+1:]...)
					}else{
						c.MapTask=[]*Task{}
					}
					
					
					//将临时文件重命名
					pattern :="mr-"+strconv.Itoa(args.TaskId)+"-[0-9]*"
					dir,err:=ioutil.ReadDir(".")
					if err!=nil{
						fmt.Println("GetReq:ReadDir fail")
					}
					for _,oldName:=range dir{
						match,_:=regexp.MatchString(pattern,oldName.Name())
						if match ==true{
							regexStr:=regexp.MustCompile("mr-[0-9]*-[0-9]*")
							newName:=regexStr.FindStringSubmatch(oldName.Name())
							os.Rename(oldName.Name(),newName[len(newName)-1])
						}
					}
					if(c.MapFiles[args.FileName]==0){
						c.MapFiles[args.FileName]=1
						c.MapFinish++
					}
					break
				}
			}
		}
		reply.TaskType=0;
	} else if args.Status == 2 {
		//reduce任务完成
		for key, value := range c.ReduceTask {
			if value.TaskId == args.TaskId {
				// fmt.Println("Reduce finish",args.TaskId)
				if(len(c.ReduceTask)>1){
					c.ReduceTask = append(c.ReduceTask[:key], c.ReduceTask[key+1:]...)
				}else{
					c.ReduceTask=[]*Task{}
				}
				
				//将临时文件重命名
				regexStr:=regexp.MustCompile("mr-out-[0-9]*")
				newName:=regexStr.FindStringSubmatch(args.FileName)
				os.Rename(args.FileName,newName[len(newName)-1])
				if(c.ReduceFiles[value.ReduceId]==0){
					//可能会出现一个reduce任务超时，之后又有一个reduce任务执行同样的文件。当done在等待时，超时的reduce任务又完成了。此时会出现重复的行
					c.ReduceFinish++
					c.ReduceFiles[value.ReduceId]=1
				}
				break
			}
		}
		if( c.Finish==true || c.NReduce==c.ReduceFinish){
			reply.TaskType=3
		}else{
			reply.TaskType=0
		}
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.ReduceFinish==c.NReduce{
		c.Finish=true
		// fmt.Println("coordinator finish")
	}
	time.Sleep(200*time.Millisecond)
	if c.Finish==true{
		dir,err:=ioutil.ReadDir(".")
		if err!=nil{
			fmt.Println("Done error",err)
		}
		var fileList []string
		pattern:="mr-out-[0-9]*-[0-9]*"
		for _,dirFiles:=range dir{
			match,_:=regexp.MatchString(pattern,dirFiles.Name())
			if match==true{
				fileList=append(fileList,dirFiles.Name())
			}
		}
		for _,fileName:=range fileList{
			err:=os.Remove(fileName)
			if err!=nil{
				fmt.Println("remove err",err)
			}
		}
	}
	return c.Finish
}

func (c *Coordinator) DetectorCrash(){
	for{
		idx:=0
		for{
			c.Mutex.Lock()
			// println("DetectorCrash",len(c.MapTask))
			if idx>=len(c.MapTask){
				c.Mutex.Unlock()
				break;
			}
			// println(time.Since(c.MapTask[idx].BeginTime))
			if time.Since(c.MapTask[idx].BeginTime)>time.Duration(10)*time.Second{
				// fmt.Println("Map timeout,len=",len(c.MapTask))
				c.MapFiles[c.MapTask[idx].FileName]=-1
				if(len(c.MapTask)>1){
					c.MapTask=append(c.MapTask[:idx],c.MapTask[idx+1:]...)
				}else{
					c.MapTask=[]*Task{}
				}
				// fmt.Println("Map timeout finish")
			}else{
				idx++
			}
			c.Mutex.Unlock()
		}
		idx=0
		for{
			c.Mutex.Lock()
			if idx>=len(c.ReduceTask){
				c.Mutex.Unlock()
				break;
			}
			if time.Since(c.ReduceTask[idx].BeginTime)>time.Duration(10)*time.Second{
				// fmt.Println("Reduce timeout",c.ReduceTask[idx].ReduceId)
				c.ReduceFiles[c.ReduceTask[idx].ReduceId]=-1
				if(len(c.ReduceTask)>1){
					c.ReduceTask=append(c.ReduceTask[:idx],c.ReduceTask[idx+1:]...)
				}else{
					c.ReduceTask=[]*Task{}
				}
			}else{
				idx++
			}
			c.Mutex.Unlock()
		}
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapFiles=make(map[string]int)

	// Your code here.
	for _, value := range files {
		c.MapFiles[value] = -1 //-1表示未分配，0表示已分配，1表示完成
	}
	
	c.ReduceFiles=make([]int,nReduce)
	for i:=0;i<nReduce;i++{
		c.ReduceFiles[i]=-1
	}

	c.NReduce = nReduce
	c.MapFinish = 0
	c.ReduceFinish = 0
	c.MapIdx = 0
	c.ReduceIdx = 0

	c.server()
	go c.DetectorCrash()
	return &c
}
