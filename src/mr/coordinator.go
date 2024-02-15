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

type Coordinator struct {
	Mu          sync.Mutex // the lock
	M           int        // total number of Map() tasks
	N           int        // total number of Reduce() tasks
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
	Status      string // "pending", "in-progress", "complete", "failed"
	Attempt     int
	Filename    string
	Id          int
	MaxDuration time.Duration
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

	c.ReduceState.Mu.Lock()
	// fmt.Println("MapState.AllDone, ReduceState.AllDone: ", c.MapState.AllDone, c.ReduceState.AllDone)
	ret = c.ReduceState.AllDone
	if ret {
		fmt.Println("~~~~~~~~~~~Coordinator ends here with c.ReduceState.AllDone == true~~~~~~~~~~~")

	}
	c.ReduceState.Mu.Unlock()

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
			Status:      "pending",
			Attempt:     0,
			Filename:    files[i],
			Id:          i,
			MaxDuration: time.Second * 10,
		}
	}

	// Initialize reduce tasks
	for i := 0; i < c.N; i++ {
		c.ReduceState.Tasks[i] = &TaskState{
			Status:      "pending",
			Attempt:     0,
			Filename:    "",
			Id:          i,
			MaxDuration: time.Second * 10,
		}
	}

	c.server()
	return c
}

func (c *Coordinator) GetMap(args *GetMapArgs, reply *GetMapReply) error {
	c.MapState.Mu.Lock()
	var id int = c.findAvailableMap()
	if c.MapState.AllDone {
		c.MapState.Mu.Unlock()
		return nil
	}
	fmt.Printf("%v | GetMap() -> %d | Current Status: ", time.Now().Format("15:04:05"), id)
	for id, task := range c.MapState.Tasks {
		fmt.Printf(" %d-%s ", id, task.Status)
	}
	fmt.Printf("\n")
	c.MapState.Mu.Unlock()

	if id != -1 { // case 1. "pending" or "failed" available when Worker calls GetMap()
		c.Mu.Lock()
		c.MapState.Mu.Lock()
		// reply
		reply.M = c.M
		reply.N = c.N
		reply.X = id
		reply.Filename = c.MapState.Tasks[id].Filename
		reply.AllMapDone = c.MapState.AllDone
		reply.StartTime = time.Now()
		// update c
		task := c.MapState.Tasks[id]
		task.Status = "in-progress"
		task.Attempt = task.Attempt + 1
		c.Mu.Unlock()
		c.MapState.Mu.Unlock()

		// set timeout
		go c.setMapTimeout(id, task.Attempt)
		return nil
	} else { // case 2. NO "pending" or "failed" available when Worker calls GetMap()
		c.MapState.Mu.Lock()
		fmt.Printf("	GetMap() -> %d NO_MAP_AVAILABLE. Sleep...\n", id)
		for c.findAvailableMap() == -1 && !c.MapState.AllDone {
			c.MapState.Cond.Wait() // Zzzzzz
		}
		/*
			What will wake up the Cond()?
				(a). Cond.Broadcast():
					- All Map() have been done
				(b). Cond.Signal():
					- When another worker returns and in rejected_case_2

			Question: Load balancing? Could it be possible that a worker is never assigned a task in its lifecycle???
		*/
		// (a) First check whether isAllMapDone()
		fmt.Printf("	GetMap() -> %d wakes up. MapState.AllDone: %v\n", id, c.MapState.AllDone)
		if c.MapState.AllDone {
			reply.AllMapDone = true
			c.MapState.Mu.Unlock()

			return nil
		} else {
			// (b) Reassign the task
			id := c.findAvailableMap()
			if id == -1 { // Is it possible that the newly returned "failed" task be taken by another Map() by getMap()??
				log.Fatal("BUG: A Map() becomes `failed` because of setMapTimeout(). But when waking up a goroutine, the goroutine cannot find it")
			}
			// task
			task := c.MapState.Tasks[id]
			task.Status = "in-progress"
			task.Attempt += 1
			// reply
			reply.AllMapDone = false
			reply.Filename = task.Filename
			reply.M = c.M
			reply.N = c.N
			reply.X = id
			reply.StartTime = time.Now()
			c.MapState.Mu.Unlock()

			go c.setMapTimeout(id, task.Attempt)
			return nil
		}
	}
}

func (c *Coordinator) PushMap(args *PushMapArgs, reply *PushMapReply) error {
	c.MapState.Mu.Lock()
	var id int = args.X
	task := c.MapState.Tasks[id]
	var accepted bool = time.Now().Before(args.StartTime.Add(task.MaxDuration)) && args.Successful

	fmt.Printf("%v | PushMap(%d) | Current Status: ", time.Now().Format("15:04:05"), id)
	for id, task := range c.MapState.Tasks {
		fmt.Printf(" %d-%s ", id, task.Status)
	}
	fmt.Printf("\n")
	fmt.Println("	Accepted? ", accepted)

	if accepted {
		task.Status = "complete"
		if c.isAllMapDone() { // the last Map to complete
			c.MapState.AllDone = true
			reply.AllMapDone = true // remember that the default reply is false
			fmt.Println("All map have been done. broadcasting...")
			c.MapState.Cond.Broadcast() // Wake all the blocked worker. They should be able to find out "It's time to Reduce()""
			c.MapState.Mu.Unlock()
			return nil
		} else {
			c.MapState.Mu.Unlock()
			return nil
		}
	} else { // rejected
		/* rejected iff
		case 1. When a Worker finally return 999 seconds later...
		case 2. When a Worker return < 10 seconds, but with args.Successful == false...
		*/
		if !time.Now().Before(args.StartTime.Add(task.MaxDuration)) { // case 1
			// need to check if all done
			// no need to reassign this task. It should be handled by the 10 seconds timer.
			if c.isAllMapDone() {
				reply.AllMapDone = true
				c.MapState.AllDone = true
				c.MapState.Mu.Unlock()
				return nil
			} else {
				c.MapState.Mu.Unlock()
				return nil
			}
		} else { // case 2
			// do I need to signal this? Could there be a lost signal?
			// TODO
			c.MapState.Cond.Signal()
			c.MapState.Mu.Unlock()
			return nil
		}
	}
}

func (c *Coordinator) isAllMapDone() bool { // Helper function. Do not lock again in this function. But make sure that MapState has been lock when calling it.

	for _, task := range c.MapState.Tasks {
		if task.Status != "complete" {
			return false
		}
	}
	return true
}

func (c *Coordinator) findAvailableMap() int { // Helper function. Do not lock again in this function. But make sure that MapState has been lock when calling it.

	for id, task := range c.MapState.Tasks {
		if task.Status == "pending" || task.Status == "failed" {
			return id
		}
	}
	return -1
}

func (c *Coordinator) setMapTimeout(id int, launched_by_attempt int) { // Make sure that MapState is NOT Locked when calling it!!!
	c.MapState.Mu.Lock()
	task := c.MapState.Tasks[id]
	c.MapState.Mu.Unlock()

	time.Sleep(task.MaxDuration) // 10 seconds later...

	c.MapState.Mu.Lock()
	defer c.MapState.Mu.Unlock()
	if c.MapState.AllDone {
		return
	}

	/*
		What if we sent a PushReduce with Successful=false before 10 seconds ?
			- solution 1. cancel this timeout event. channel? time.ticker? ...
			- solution 2. try to detect this case in timeout. (what I'm doing here)
	*/

	if task.Attempt == launched_by_attempt && task.Status == "in-progress" { // worker timeout
		task.Status = "failed"
		c.MapState.Cond.Signal()
	} else if task.Attempt == launched_by_attempt && task.Status == "failed" { // worker crashed within 10 seconds && no other task attempted
		return // Status update should be handled by PushMap
	} else { // worker crashed within 10 seconds && other task attempted
		return // Status update should be handled by the last task
	}
}

/*
	Reduce.
	// Generally the code are the same. Be careful of typo here.
*/

func (c *Coordinator) GetReduce(args *GetReduceArgs, reply *GetReduceReply) error {
	c.ReduceState.Mu.Lock()
	var id int = c.findAvailableReduce()
	if c.ReduceState.AllDone {
		c.ReduceState.Mu.Unlock()
		return nil
	}
	fmt.Printf("%v | GetReduce() -> %d | Current Status: ", time.Now().Format("15:04:05"), id)
	for id, task := range c.ReduceState.Tasks {
		fmt.Printf(" %d-%s ", id, task.Status)
	}
	fmt.Printf("\n")
	c.ReduceState.Mu.Unlock()

	if id != -1 { // case 1. "pending" or "failed" available when Worker calls GetReduce()
		c.Mu.Lock()
		c.ReduceState.Mu.Lock()
		// reply
		reply.M = c.M
		reply.N = c.N
		reply.Y = id

		reply.AllReduceDone = c.ReduceState.AllDone
		reply.StartTime = time.Now()
		// update c
		task := c.ReduceState.Tasks[id]
		task.Status = "in-progress"
		task.Attempt = task.Attempt + 1
		c.Mu.Unlock()
		c.ReduceState.Mu.Unlock()

		// set timeout
		go c.setReduceTimeout(id, task.Attempt)
		return nil
	} else { // case 2. NO "pending" or "failed" available when Worker calls GetReduce()
		c.ReduceState.Mu.Lock()
		fmt.Printf("	GetReduce() -> %d: NO_REDUCE_AVAILABLE. Sleep......\n", id)
		for c.findAvailableReduce() == -1 && !c.ReduceState.AllDone {
			c.ReduceState.Cond.Wait() // Zzzzzz
		}
		/*
			What will wake up the Cond()?
				(a). Cond.Broadcast():
					- All Reduce() have been done
				(b). Cond.Signal():
					- When another worker returns and in rejected_case_2

			Question: Load balancing? Could it be possible that a worker is never assigned a task in its lifecycle???
		*/
		// (a) First check whether isAllReduceDone()

		fmt.Printf("	GetReduce() -> %d wakes up. ReduceState.AllDone: %v\n", id, c.ReduceState.AllDone)
		if c.ReduceState.AllDone {
			reply.AllReduceDone = true
			c.ReduceState.Mu.Unlock()
			return nil
		} else {
			// (b) Reassign the task
			id := c.findAvailableReduce()
			if id == -1 { // Is it possible that the newly returned "failed" task be taken by another Reduce() by getReduce()??
				log.Fatal("BUG: A Reduce() becomes `failed` because of setReduceTimeout(). But when waking up a goroutine, the goroutine cannot find it")
			}
			// task
			task := c.ReduceState.Tasks[id]
			task.Status = "in-progress"
			task.Attempt += 1
			// reply
			reply.AllReduceDone = false
			reply.M = c.M
			reply.N = c.N
			reply.Y = id
			reply.StartTime = time.Now()
			c.ReduceState.Mu.Unlock()

			go c.setReduceTimeout(id, task.Attempt)
			return nil
		}
	}

}

func (c *Coordinator) PushReduce(args *PushReduceArgs, reply *PushReduceReply) error {
	c.ReduceState.Mu.Lock()
	var id int = args.Y
	task := c.ReduceState.Tasks[id]
	var accepted bool = time.Now().Before(args.StartTime.Add(task.MaxDuration)) && args.Successful

	fmt.Printf("%v | PushReduce(%d) | Current Status: ", time.Now().Format("15:04:05"), id)
	for id, task := range c.ReduceState.Tasks {
		fmt.Printf(" %d-%s ", id, task.Status)
	}
	fmt.Printf("\n")
	fmt.Println("	Accepted? ", accepted)

	if accepted {
		task.Status = "complete"
		if c.isAllReduceDone() {
			reply.AllReduceDone = true
			c.ReduceState.AllDone = true
			c.ReduceState.Cond.Broadcast()
			c.ReduceState.Mu.Unlock()
			return nil
		} else {
			c.ReduceState.Mu.Unlock()
			return nil
		}
	} else { // rejected
		/* rejected iff
		case 1. When a Worker finally return 999 seconds later...
		case 2. When a Worker return < 10 seconds, but with args.Successful == false...
		*/
		if !time.Now().Before(args.StartTime.Add(task.MaxDuration)) { // case 1
			// need to check if all done
			// no need to reassign this task. It should be handled by the 10 seconds timer.
			if c.isAllReduceDone() {
				reply.AllReduceDone = true
				c.ReduceState.AllDone = true
				c.ReduceState.Mu.Unlock()
				return nil
			} else {
				c.ReduceState.Mu.Unlock()
				return nil
			}
		} else { // case 2
			// do I need to signal this? Could there be a lost signal?
			// TODO
			c.ReduceState.Cond.Signal()
			c.ReduceState.Mu.Unlock()
			return nil
		}
	}
}

func (c *Coordinator) isAllReduceDone() bool { // Helper function. Do not lock again in this function. But make sure that ReduceState has been lock when calling it.
	for _, task := range c.ReduceState.Tasks {
		if task.Status != "complete" {
			return false
		}
	}
	return true
}

func (c *Coordinator) findAvailableReduce() int { // Helper function. Do not lock again in this function. But make sure that ReduceState has been lock when calling it.
	for id, task := range c.ReduceState.Tasks {
		if task.Status == "pending" || task.Status == "failed" {
			return id
		}
	}
	return -1
}

func (c *Coordinator) setReduceTimeout(id int, launched_by_attempt int) { // Make sure that ReduceState is NOT locked when calling this function
	c.ReduceState.Mu.Lock()
	task := c.ReduceState.Tasks[id]
	c.ReduceState.Mu.Unlock()

	time.Sleep(task.MaxDuration) // 10 seconds later...

	c.ReduceState.Mu.Lock()
	defer c.ReduceState.Mu.Unlock()
	if c.ReduceState.AllDone {
		return
	}

	/*
		What if we have sent a PushReduce with Successful=false before 10 seconds ?
			- solution 1. cancel this timeout event. channel? time.ticker? ...
			- solution 2. try to detect this in timeout. (what I'm doing here)
	*/
	if task.Attempt == launched_by_attempt && task.Status == "in-progress" { // worker timeout
		task.Status = "failed"
		c.ReduceState.Cond.Signal()
	} else if task.Attempt == launched_by_attempt && task.Status == "failed" { // worker crashed within 10 seconds && no other task attempted
		return // Status update should be handled by PushReduce
	} else { // worker crashed within 10 seconds && other task attempted
		return // Status update should be handled by the last task
	}
}
