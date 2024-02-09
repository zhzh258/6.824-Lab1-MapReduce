package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// mu            sync.Mutex // the lock
	m             int // total number of Map()
	n             int // total number of Reduce()
	mapCounter    int // number of Map() done
	reduceCounter int // number of Reduce() done
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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
	c := Coordinator{}

	// Your code here.
	c.m = len(files)
	c.n = nReduce

	c.server()
	return &c
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
