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
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RunMap(arg *CoordinatorReply, mapf func(string, string) []KeyValue) bool {
	fileName := arg.FileName
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

	// 创建NReduce文件的map映射
	tmpMap:=make(map[int](*os.File))
	for idx:=0;idx<arg.NReduce;idx++{
		filePrefix:="mr-"+strconv.Itoa(arg.TaskId)+"-"+strconv.Itoa(idx)+"-"
		tmpFile,err2:=ioutil.TempFile(".",filePrefix)
		if err2!=nil{
			fmt.Println("create temp_file error")
			return false
		}
		tmpMap[idx]=tmpFile
	}

	//将key-value写入到中间文件中
	for _, value := range kva {
		idx := ihash(value.Key)%arg.NReduce
		encoder := json.NewEncoder(tmpMap[idx])
		err = encoder.Encode(&value)
		if err != nil {
			fmt.Println("Encode error")
			return false
		}
	}

	for _,value:= range tmpMap{
		 value.Close()
	}
	return true
}

func RunReduce(arg *CoordinatorReply, reducef func(string, []string) string) bool {
	//先获取该目录下的所有指定的文件，接着使用reducef
	fileNames:="mr-"+"*"+"-"+strconv.Itoa(arg.ReduceId)
	fileList := GetFilesFromDir(fileNames)
	var kva []KeyValue
	for _,fileName := range fileList{
		file,_ :=os.Open(fileName)
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err:=dec.Decode(&kv);err!=nil{
				break
			}
			kva=append(kva,kv)
		}
	}
	sort.Sort(ByKey(kva))
	//将文件写到中间文件
	filePrefix:="mr-out-"+strconv.Itoa(arg.ReduceId)+"-"
	tmpFile,err:=ioutil.TempFile(".",filePrefix)
	if err!=nil{
		fmt.Println("reduce create tmp_file error")
		return false
	}

	i:=0
	for i<len(kva){
		j:=i+1
		for j<len(kva)&&kva[j].Key==kva[i].Key{
			j++
		}
		values:=[]string{}
		for k:=i;k<j;k++{
			values=append(values,kva[k].Value)
		}
		output:=reducef(kva[i].Key,values)
		fmt.Fprintf(tmpFile	, "%v %v\n", kva[i].Key, output)
		i=j
	}
	tmpFile.Close()
	return true
}

//
// main/mrworker.go calls this function.
//给coordinator发送一个rpc请求任务（callExample）。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for{
		args := WorkerAsk{}
		args.Status = 0
		reply := CoordinatorReply{}
		if call("Coordinator.GetReq", &args, &reply) {
			if reply.TaskType == 1 {
				if RunMap(&reply, mapf) == true {
					//向coordinator告知map结束
					finishReq := WorkerAsk{1, reply.FileName, reply.TaskId}
					invalidReply := CoordinatorReply{}
					call("Coordinator.GetReq", finishReq, invalidReply)
				}

			} else if reply.TaskType == 2 {
				if RunReduce(&reply,reducef)==true{
					finishReq := WorkerAsk{2,"",reply.TaskId}
					invalidReply:=CoordinatorReply{}
					call("Coordinator.GetReq", finishReq, invalidReply)
				}
			}
		}
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
