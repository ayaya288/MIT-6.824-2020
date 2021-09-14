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
	mux sync.Mutex

	mapTasksReady      map[int]Task
	mapTasksInProgress map[int]Task

	reduceTasksReady      map[int]Task
	reduceTasksInProgress map[int]Task

	reduceReady bool
	nReduce     int
	nMap        int
}

type Task struct {
	Filename string
	TaskType int
	TaskID   int

	NReduce int
	NMap    int

	TimeStamp int64
}

// Task type
const (
	Map    = 0
	Reduce = 1
	Wait   = 2
	Done   = 3
)

// Your code here -- RPC handlers for the worker to call.
// Collect tasks in progress more than (delay) seconds,
func (m *Master) collectStallTasks() {
	const delay int64 = 10
	curTime := time.Now().Unix()
	for k, v := range m.mapTasksInProgress {
		if curTime-v.TimeStamp > delay {
			m.mapTasksReady[k] = v
			delete(m.mapTasksInProgress, k)
			fmt.Printf("Collect map task %d\n", k)
		}
	}
	for k, v := range m.reduceTasksInProgress {
		if curTime-v.TimeStamp > delay {
			m.reduceTasksReady[k] = v
			delete(m.mapTasksInProgress, k)
			fmt.Printf("Collect reduce task %d\n", k)
		}
	}
}

// RPC: called by worker to get a task from master
func (m *Master) GetTask(args *RPCArgs, reply *RPCReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.collectStallTasks()

	if !m.reduceReady {
		if len(m.mapTasksReady) > 0 {
			for k, v := range m.mapTasksReady {
				v.TimeStamp = time.Now().Unix()
				reply.TaskInfo = v
				m.mapTasksInProgress[k] = v
				delete(m.mapTasksReady, k)
				fmt.Printf("Handout map task %d\n", reply.TaskInfo.TaskID)
				return nil
			}
		} else if len(m.mapTasksInProgress) > 0 {
			reply.TaskInfo = Task{TaskType: Wait}
			return nil
		} else {
			for i := 0; i < m.nReduce; i++ {
				m.reduceTasksReady[i] = Task{
					TaskType:  Reduce,
					TaskID:    i,
					NReduce:   m.nReduce,
					NMap:      m.nMap,
					TimeStamp: time.Now().Unix()}
			}
			m.reduceReady = true
		}
	}

	if len(m.reduceTasksReady) > 0 {
		for k, v := range m.reduceTasksReady {
			v.TimeStamp = time.Now().Unix()
			reply.TaskInfo = v
			m.reduceTasksInProgress[k] = v
			delete(m.reduceTasksReady, k)
			fmt.Printf("Handout reduce task %d\n", k)
			return nil
		}
	} else if len(m.reduceTasksInProgress) > 0 {
		reply.TaskInfo = Task{TaskType: Wait}
	} else {
		reply.TaskInfo = Task{TaskType: Done}
	}
	return nil
}

// RPC: called by worker to notify task done
func (m *Master) TaskDone(args *RPCArgs, reply *RPCReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	switch args.TaskInfo.TaskType {
	case Map:
		delete(m.mapTasksInProgress, args.TaskInfo.TaskID)
		fmt.Printf("Map task %d done, %d tasks left\n",
			args.TaskInfo.TaskID, len(m.mapTasksInProgress)+len(m.mapTasksReady))
	case Reduce:
		delete(m.reduceTasksInProgress, args.TaskInfo.TaskID)
		fmt.Printf("Reduce task %d done, %d tasks left\n",
			args.TaskInfo.TaskID, len(m.reduceTasksInProgress)+len(m.reduceTasksReady))
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

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
	if len(m.mapTasksReady) == 0 && len(m.mapTasksInProgress) == 0 &&
		len(m.reduceTasksReady) == 0 && len(m.mapTasksInProgress) == 0 {
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
	//Initial
	m.mapTasksReady = make(map[int]Task)
	m.mapTasksInProgress = make(map[int]Task)
	m.reduceTasksReady = make(map[int]Task)
	m.reduceTasksInProgress = make(map[int]Task)

	numFile := len(files)

	for i, file := range files {
		m.mapTasksReady[i] = Task{
			Filename:  file,
			TaskType:  Map,
			TaskID:    i,
			NReduce:   nReduce,
			NMap:      numFile,
			TimeStamp: time.Now().Unix(),
		}
	}
	m.reduceReady = false
	m.nReduce = nReduce
	m.nMap = numFile

	m.server()
	return &m
}
