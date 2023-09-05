package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"


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


func runMap(arg *CoordinatorReply,mapf func(string,string) []KeyValue) bool{
	fileName:=arg.fileName
	file,err := os.Open(fileName)
	if err!=nil{
		log.Fatalf("cannot open %v",fileName)
		return false
	}
	content,err:=ioutil.ReadAll(file)
	if err!=nil{
		log.Fatalf("cannot read %v",fileName)
		return false
	}
	file.Close()
	kva:=mapf(fileName,string(content))
	//将key-value写入到中间文件中

	return true
}

func runReduce(arg *CoordinatorReply,reducef func(string,[]string) string) bool{
	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	
	// uncomment to send the Example RPC to the coordinator.
	reply := CallExample()
	//给coordinator发送一个RPC请求一个任务。
	//读取文件内容，并调用Map函数
	
	if(reply.taskType == 0){
		if(runMap(reply,mapf)==true){
			//告诉coordinator已经完成该任务。返回文件名
		}
	}else{
		if(runReduce(reply,reducef)==true){
			//告诉coordinator已经完成该任务
		}
	}
}




//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() *CoordinatorReply{

	// declare an argument structure.
	args := WorkerAsk{}


	// declare a reply structure.
	reply :=CoordinatorReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.getReq", &args, &reply)

	return &reply
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
