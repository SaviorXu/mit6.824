package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func runMap(arg *CoordinatorReply, mapf func(string, string) []KeyValue) bool {
	fileName := arg.fileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return false
	}
	file.Close()
	kva := mapf(fileName, string(content))
	//将key-value写入到中间文件中
	for _, value := range kva {
		idx := ihash(value.Key)
		interFileName := "mr-" + strconv.Itoa(arg.taskId) + "-" + strconv.Itoa(idx)
		file, err := os.OpenFile(interFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			fmt.Println("intermediate file open error", err)
			return false
		}
		encoder := json.NewEncoder(file)
		err = encoder.Encode(&value)
		if err != nil {
			fmt.Println("Encode error")
			return false
		}
	}
	return true
}

func runReduce(arg *CoordinatorReply, reducef func(string, []string) string) bool {
	//先获取该目录下的所有指定的文件，接着使用reducef
}

//
// main/mrworker.go calls this function.
//给coordinator发送一个rpc请求任务（callExample）。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	args := WorkerAsk{}
	args.status = 0
	reply := CoordinatorReply{}
	if call("Coordinator.getReq", &args, &reply) {
		if reply.taskType == 0 {
			if runMap(&reply, mapf) == true {
				//向coordinator告知map结束
				finishReq := WorkerAsk{1, reply.fileName, reply.taskId}
				invalidReply := CoordinatorReply{}
				call("Coordinator.getReq", finishReq, invalidReply)
			}

		} else if reply.taskType == 1 {

		}
	}
}

func OnlyCall(args *WorkerAsk, reply *CoordinatorReply) *CoordinatorReply {
	call("Coordinator.getReq", &args, &reply)
	if reply.taskType == 0 {

	} else if reply.taskType == 1 {

	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
