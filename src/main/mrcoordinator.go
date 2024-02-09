package main

/*

	1. The input has been divided to many pieces. Namely, the many pg-*.txt files. (assume m files)
	2. The coordinator reads each file (each Map task), and assign them to a worker goroutine
	3. worker.count() = 3 < Map.count(), Reduce.count()
	4. Map. There are m Map tasks to do:
		- Map ID: X = 1, 2, ..., m
		- input: pg-X.txt
		- output: n intermediate files. (each Map task produces n intermediate JSON files)
			- mr-X-[1..n]
			- X is the Map id, Y is the Reduce id

		- How to the coordinator/worker know Map has been done?
			- Request, response

		- Totally m*n intermediate files generated

	5, Reduce. There are n Reduce tasks to do
		- Reduce ID: Y = 1, 2, ..., n
		- input: mr-[1..m]-Y
		- output: mr-out-Y

		- Totally m*n intermediate files read

	6. coordinator.Done() -> true.
		- How do a worker goroutine know it shall exit?
			- "please exit" pseudotask
*/

// $ rm mr-out*
// $ go run -race mrcoordinator.go pg-*.txt

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
