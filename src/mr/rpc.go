package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
	Filename string
	M        int
	N        int
	X        int // the Map task id

}

type PushMapArgs struct {
}

type PushMapReply struct {
}

type GetReduceArgs struct {
}

type GetReduceReply struct {
	M int
	N int
	Y int // the Reduce task id
}

// Add your RPC definitions here.
type PushReduceArgs struct {
}

type PushReduceReply struct {
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
