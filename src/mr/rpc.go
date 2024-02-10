package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetMapArgs struct {
}

type GetMapReply struct {
	Filename   string
	M          int
	N          int
	X          int // the Map task id
	AllMapDone bool
	StartTime  time.Time
}

type PushMapArgs struct {
	X          int
	Successful bool // true if Worker.Map() succeeded to execute
	StartTime  time.Time
}

type PushMapReply struct {
	AllMapDone bool
}

type GetReduceArgs struct {
}

type GetReduceReply struct {
	M             int
	N             int
	Y             int // the Reduce task id
	AllReduceDone bool
	StartTime     time.Time
}

type PushReduceArgs struct {
	Y          int
	Successful bool // true if Worker.Reduce() succeeded to execute
	StartTime  time.Time
}

type PushReduceReply struct {
	AllReduceDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
