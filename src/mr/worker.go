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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	debug_map_counter := 0
	for { // Map task.
		debug_map_counter++
		fmt.Println("debug_map_counter: ", debug_map_counter)
		// 1. GetMap()
		getArgs := GetMapArgs{}
		getReply := GetMapReply{}
		getok := call("Coordinator.GetMap", &getArgs, &getReply)
		if getok {
			fmt.Printf("worker successfully fetched new Map task (or blocked till allDone), X = %d, Filename = %s\n", getReply.X, getReply.Filename)
		} else {
			log.Fatal("Error: worker.call(coordinator.GetMap) failed. getArgs, getReply: ", getArgs, getReply)
		}
		if getReply.AllMapDone {
			break
		}
		// 2. do the map
		mapById(mapf, reducef, getReply.Filename, getReply.M, getReply.N, getReply.X)

		// 3. PushMap()
		pushArgs := PushMapArgs{X: getReply.X, Successful: true, StartTime: getReply.StartTime}
		pushReply := PushMapReply{}
		pushok := call("Coordinator.PushMap", &pushArgs, &pushReply)
		if pushok {
			fmt.Println("worker successfully sent response of a Map task")
		} else {
			log.Fatal("Error: worker.call(coordinator.PushMap) failed. pushArgs, pushReply: ", pushArgs, pushReply)
		}
		if pushReply.AllMapDone {
			break
		}
	}

	fmt.Printf("Map() all done successfully!!!!!!!!!!!!!\n\n\n\n")

	debug_reduce_counter := 0
	for { // Reduce task. Reduce-task-Id == Y. (There are N reduce tasks in total.)
		debug_reduce_counter++
		fmt.Println("debug_reduce_counter: ", debug_reduce_counter)
		// 1. GetReduce()
		getArgs := GetReduceArgs{}
		getReply := GetReduceReply{}
		getok := call("Coordinator.GetReduce", &getArgs, &getReply)
		if getok {
			fmt.Printf("worker successfully fetched new Reduce task (or blocked till allDone), Y = %d\n", getReply.Y)
		} else {
			log.Fatal("Error: worker.call(coordinator.GetReduce) failed. getArgs, getReply: ", getArgs, getReply)
		}
		if getReply.AllReduceDone {
			break
		}

		// 2. do the reduce
		reduceById(mapf, reducef, getReply.M, getReply.N, getReply.Y)

		// 3. PushReduce()
		pushArgs := PushReduceArgs{Y: getReply.Y, Successful: true, StartTime: getReply.StartTime}
		pushReply := GetReduceReply{}
		pushok := call("Coordinator.PushReduce", &pushArgs, &pushReply)
		if pushok {
			fmt.Printf("worker successfully sent response of a Reduce task, Y = %d\n", getReply.Y)
		} else {
			log.Fatal("Error: worker.call(coordinator.pushReduce) failed. pushArgs, pushReply: ", pushArgs, pushReply)
		}
		if pushReply.AllReduceDone {
			break
		}
	}

	fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n\nReduce() all done successfully!!!!!!!!!!!!!\n\n\n\n")
	fmt.Println("Worker close here......")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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

func mapById(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	Filename string,
	M int,
	N int,
	X int,
) { // do the X-th Map()
	// generate all mr-X-???
	// 2. Read the pg.txt and Map()
	file, err := os.Open(Filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v", Filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Filename)
	}
	file.Close()
	kva := mapf(Filename, string(content)) // []KeyValue. Divide this array into n buckets
	// fmt.Println("The len of mapf.return is ", len(kva))

	// 3. Create & open all the intermediate file mr-X-???
	ofiles := make([]*os.File, N) // The slice to store the entry to all mr-X-???
	encoders := make([]*json.Encoder, N)
	for Y := 0; Y < N; Y++ {
		var filepath string = fmt.Sprintf("mr-%d-%d", X, Y) // mr-X-???
		ofiles[Y], err = os.Create(filepath)
		encoders[Y] = json.NewEncoder(ofiles[Y])
		if err != nil {
			log.Fatal("Error in os.create mr-X-Y: ", err)
		}
		defer ofiles[Y].Close()
	}

	for _, kv := range kva {
		Y := ihash(kv.Key) % N

		err = encoders[Y].Encode(&kv)
		if err != nil {
			log.Fatal("Error writing the kv into intermediate file: ", err)
		}
	}
}
func reduceById(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	M int,
	N int,
	Y int,
) { // do the Y-th Map()
	// generate mr-out-Y
	// 2. Create an output file
	ofile, err := os.Create(fmt.Sprintf("mr-out-%d", Y))
	if err != nil {
		fmt.Println("Error in outputing mr-output-Y in Reduce(): ", err)
	}
	defer ofile.Close()
	// 3. Read key-value pairs from mr-???-Y (intermediate files) into `kva`. Then sort and Reduce()
	kva := []KeyValue{}
	for X := 0; X < M; X++ { // totally M loops. (the Y-th Reduce() task from all the M Map())
		ifile, err := os.Open(fmt.Sprintf("mr-%d-%d", X, Y))
		if err != nil {
			fmt.Println("Error in reading mr-???-Y in Reduce(): ", err)
		}
		decoder := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		// fmt.Printf("The length of kva of file mr-%v-Y is %v\n", X, len(kva))
		ifile.Close()
	}
	sort.Sort(ByKey(kva))

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// Fprintf the result into the output file of the Y-th Reduce task. (mr-output-Y)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
}
