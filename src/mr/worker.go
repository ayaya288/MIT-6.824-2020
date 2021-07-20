package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// sorting by key
type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapfun func(string, string) []KeyValue,
	redufun func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RPCArgs{}
		reply := RPCReply{}
		call("Master.GetTask", &args, &reply) //get task from master
		switch reply.TaskInfo.TaskType {
		case Map:
			doMap(&reply.TaskInfo, mapfun)
		case Reduce:
			doReduce(&reply.TaskInfo, redufun)
		case Wait:
			time.Sleep(time.Second)
		case Stop:
			return
		}
		args.TaskInfo = reply.TaskInfo
		call("Master.TaskDone", &args, &reply)
	}

}

func doMap(task *Task, mapfun func(string, string) []KeyValue) {

	//create a slice to save intermediate
	intermediate := make([][]KeyValue, task.NumReduce)
	for i := 0; i < task.NumReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}

	//read the content of task
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatal("connot open file: ", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read file: ", task.FileName)
	}
	file.Close()

	//do map function
	kva := mapfun(task.FileName, string(content))
	//save map result to intermediate
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%task.NumReduce] =
			append(intermediate[ihash(kv.Key)%task.NumReduce], kv)
	}

	//save result to file
	for i := 0; i < task.NumMap; i++ {
		if len(intermediate[i]) == 0 {
			continue
		}

		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		ofile, _ := ioutil.TempFile("./", "tmp_")
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encode error: Key-%s, Value-%s", kv.Key, kv.Value)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

func doReduce(task *Task, redufun func(string, []string) string) {
	intermediate := make([]KeyValue, 0)

	//get all input
	for i := 0; i < task.NumMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	//sort
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)

	ofile, _ := ioutil.TempFile("./", "tmp_")

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := redufun(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
