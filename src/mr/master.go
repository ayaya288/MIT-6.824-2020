package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapTaskReady      map[int]Task
	mapTaskInProgress map[int]Task

	reduceTaskReady      map[int]Task
	reduceTaskInProgress map[int]Task

	mux sync.Mutex //protect tasks

	reduceReady bool
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	FileName  string //file path
	TaskType  int    //0: map 1: reduce
	TaskID    int
	NumMap    int
	NumReduce int
	TimeStamp int64 //record time
}

const (
	Stop   = 0
	Wait   = 1
	Map    = 2
	Reduce = 3
)

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) collectStallTasks() {
	currTime := time.Now().Unix()
	for k, v := range m.mapTaskInProgress {
		if currTime > v.TimeStamp+10 {
			m.mapTaskReady[k] = v
			delete(m.mapTaskInProgress, k)
		}
	}
	for k, v := range m.reduceTaskInProgress {
		if currTime > v.TimeStamp+10 {
			m.reduceTaskReady[k] = v
			delete(m.reduceTaskInProgress, k)
		}
	}
}

func (m *Master) GetTask(args *RPCArgs, reply *RPCReply) error {
	fmt.Println("GetTask called...")
	m.mux.Lock()
	defer m.mux.Unlock()

	//get tasks more than 10s
	m.collectStallTasks()

	//do task assignment
	if len(m.mapTaskReady) > 0 {
		for k, v := range m.mapTaskReady {
			v.TimeStamp = time.Now().Unix()
			reply.TaskInfo = v
			m.mapTaskInProgress[k] = v
			delete(m.mapTaskReady, k)
			return nil
		}
	} else if len(m.mapTaskInProgress) > 0 {
		reply.TaskInfo = Task{TaskType: Wait}
		return nil
	}

	if !m.reduceReady {
		for i := 0; i < m.nReduce; i++ {
			m.reduceTaskReady[i] = Task{
				TaskType:  Reduce,
				TaskID:    i,
				NumMap:    m.nMap,
				NumReduce: m.nReduce,
				TimeStamp: time.Now().Unix(),
			}
		}
		m.reduceReady = true
	}

	if len(m.reduceTaskReady) > 0 {
		for k, v := range m.reduceTaskReady {
			v.TimeStamp = time.Now().Unix()
			reply.TaskInfo = v
			m.reduceTaskInProgress[k] = v
			delete(m.reduceTaskReady, k)
			return nil
		}
	} else if len(m.reduceTaskInProgress) > 0 {
		reply.TaskInfo = Task{TaskType: Wait}
	} else {
		reply.TaskInfo = Task{TaskType: Stop}
	}

	return nil
}

func (m *Master) TaskDone(args *RPCArgs, reply *RPCReply) error {
	fmt.Printf("%d done...", args.TaskInfo.TaskID)
	m.mux.Lock()
	defer m.mux.Unlock()

	switch args.TaskInfo.TaskType {
	case Map:
		delete(m.mapTaskInProgress, args.TaskInfo.TaskID)
	case Reduce:
		delete(m.reduceTaskInProgress, args.TaskInfo.TaskID)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.mapTaskReady) == 0 && len(m.mapTaskInProgress) == 0 &&
		len(m.reduceTaskReady) == 0 && len(m.reduceTaskInProgress) == 0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mux.Lock()
	defer m.mux.Unlock()

	m.mapTaskReady = make(map[int]Task)
	m.mapTaskInProgress = make(map[int]Task)
	m.reduceTaskReady = make(map[int]Task)
	m.reduceTaskInProgress = make(map[int]Task)

	numFile := len(files)
	for i, file := range files {
		m.mapTaskReady[i] = Task{
			FileName:  file,
			TaskType:  Map,
			TaskID:    i,
			NumMap:    numFile,
			NumReduce: nReduce,
			TimeStamp: time.Now().Unix(),
		}
	}
	m.reduceReady = false
	m.nMap = numFile
	m.nReduce = nReduce

	m.server()
	return &m
}
