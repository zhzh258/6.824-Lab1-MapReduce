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

type Coordinator struct {
	// mu            sync.Mutex // the lock
	M           int // total number of Map() tasks
	N           int // total number of Reduce() tasks
	MapState    MapReduceState
	ReduceState MapReduceState
}

type MapReduceState struct {
	Tasks   map[int]*TaskState
	Mu      sync.Mutex
	Cond    *sync.Cond
	AllDone bool
}

type TaskState struct {
	Status    string // "pending", "in-progreee", "complete", "failed"
	Id        int
	StartTime time.Time
	Duration  time.Duration
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() // the socket name is shared by storing it in rpc.go
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		M: len(files),
		N: nReduce,
		MapState: MapReduceState{
			Tasks:   make(map[int]*TaskState),
			Mu:      sync.Mutex{},
			AllDone: false,
		},
		ReduceState: MapReduceState{
			Tasks:   make(map[int]*TaskState),
			Mu:      sync.Mutex{},
			AllDone: false,
		},
	}

	c.MapState.Cond = sync.NewCond(&c.MapState.Mu)
	c.ReduceState.Cond = sync.NewCond(&c.ReduceState.Mu)

	// Initialize map tasks
	for i := 0; i < c.M; i++ {
		c.MapState.Tasks[i] = &TaskState{
			Status: "Pending",
			Id:     i,
			// StartTime and Duration can be set when the task is actually started
		}
	}

	// Initialize reduce tasks
	for i := 0; i < c.N; i++ {
		c.ReduceState.Tasks[i] = &TaskState{
			Status: "Pending",
			Id:     i,
			// StartTime and Duration can be set when the task is actually started
		}
	}

	c.server()
	return c
}

// Assumptions (for now):
// 1. Workers never crashes
// 2. No worker will take extra long time

func (c *Coordinator) GetMap(args *GetMapArgs, reply *GetMapReply) error {

	// fmt.Println("Now running Coordinator.GetMap()...")
	reply.Filename = "pg-being_ernest.txt"
	reply.M = 1
	reply.N = 3
	reply.X = 0 // It tells the worker which Map() to do
	return nil
}

func (c *Coordinator) GetReduce(args *GetReduceArgs, reply *GetReduceReply) error {
	// fmt.Println("Now running Coordinator.GetReduce()...")
	reply.M = 1
	reply.N = 3
	reply.Y = 0 // It tells the worker which Reduce() to do
	return nil
}

func (c *Coordinator) PushMap(args *PushMapArgs, reply *PushMapReply) error {
	return nil
}

func (c *Coordinator) PushReduce(args *PushReduceArgs, reply *PushReduceReply) error {
	return nil
}
