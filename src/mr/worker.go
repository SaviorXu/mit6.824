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
	"regexp"
	"path/filepath"
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

func RunMap(arg *CoordinatorReply, mapf func(string, string) []KeyValue){
	// fmt.Println("RunMap")
	fileName := arg.FileName
	file, err := os.Open(fileName)
	// fmt.Println("os.Open")
	if err != nil {
		// fmt.Printf("filename=%s",fileName)
		log.Fatalf("RunMap:cannot open %v", fileName)
		return
	}
	content, err := ioutil.ReadAll(file)
	// fmt.Println("ioutil.ReadAll")
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return
	}
	file.Close()
	kva := mapf(fileName, string(content))

	// 创建NReduce文件的map映射
	tmpMap:=make(map[int](*os.File))
	for idx:=0;idx<arg.NReduce;idx++{
		filePrefix:="mr-"+strconv.Itoa(arg.TaskId)+"-"+strconv.Itoa(idx)+"-"
		tmpFile,err2:=ioutil.TempFile(".",filePrefix)
		if err2!=nil{
			fmt.Println("create temp_file error",err2)
			return
		}
		tmpMap[idx]=tmpFile
	}
	// fmt.Println("write key_value")
	//将key-value写入到中间文件中
	for _, value := range kva {
		idx := ihash(value.Key)%arg.NReduce
		// fmt.Println("arg.NReduce",arg.NReduce)
		encoder := json.NewEncoder(tmpMap[idx])
		err = encoder.Encode(&value)
		if err != nil {
			fmt.Println("Encode error",err)
			return
		}
	}

	for _,value:= range tmpMap{
		 value.Close()
	}
	// fmt.Println("map finish")

	finishReq := WorkerAsk{1, arg.FileName, arg.TaskId}
	invalidReply := CoordinatorReply{}
	call("Coordinator.GetReq", &finishReq, &invalidReply)
}

func RunReduce(arg *CoordinatorReply, reducef func(string, []string) string) {
	//先获取该目录下的所有指定的文件，接着使用reducef
	// fmt.Println("worker.go:RunReduce")
	dir,err:=ioutil.ReadDir(".")
	if err!=nil{
		fmt.Println("RunReduce:ReadDir error",err)
		return 
	}
	var fileList []string
	// fmt.Println("worker.go:arg.ReduceId",strconv.Itoa(arg.ReduceId))
	pattern := "mr-"+"[0-9]*"+"-"+strconv.Itoa(arg.ReduceId)+"$"
	for _,dirFiles:=range dir{
		match,_:=regexp.MatchString(pattern,dirFiles.Name())
		if match ==true{
			// fmt.Println("worker.go:match",strconv.Itoa(arg.ReduceId))
			fileList=append(fileList,dirFiles.Name())
		}
	}
	var kva []KeyValue
	for _,fileName := range fileList{
		file,err :=os.Open(fileName)
		if err!=nil{
			fmt.Println("RunReduce open error",fileName)
			return 
		}
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err :=dec.Decode(&kv);err!=nil{
				// fmt.Println("decode error",fileName,err)
				break
			}
			kva=append(kva,kv)
		}
	}
	// fmt.Println("arg.ReduceId:",arg.ReduceId," len(kva)=",len(kva))
	sort.Sort(ByKey(kva))
	//将文件写到中间文件
	filePrefix:="mr-out-"+strconv.Itoa(arg.ReduceId)+"-"
	tmpFile,err:=ioutil.TempFile(".",filePrefix)
	if err!=nil{
		fmt.Println("reduce create tmp_file error")
		return 
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
	_,postfix:=filepath.Split(tmpFile.Name())
	// fmt.Println("worker.go:RunReduce finish",strconv.Itoa(arg.ReduceId)," len(kva)=",len(kva)," arg.TaskId=",arg.TaskId," tmpFile=",postfix)
	
	finishReq := WorkerAsk{2,postfix,arg.TaskId}
	invalidReply:=CoordinatorReply{}
	call("Coordinator.GetReq", &finishReq, &invalidReply)
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
			if reply.TaskType==3{
				break
			}else if reply.TaskType == 1 {
				RunMap(&reply, mapf)
			} else if reply.TaskType == 2 {
				RunReduce(&reply,reducef)
			}else if reply.TaskType==0{
				continue
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
