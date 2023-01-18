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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type bucket []KeyValue

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (reply *Reply) reset() {
	*reply = Reply{}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	fmt.Println("created")
	CallToCoordinator(mapf, reducef)

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallToCoordinator(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// declare an argument structure.
	args := RequestArgs{NeedJob: true, JobDone: false}
	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	callResult := call("Coordinator.WorkDistributor", &args, &reply)
	if callResult {
		if reply.JobType == MAP_JOB {
			//fmt.Println("1got a map job!")
			myMap(&args, &reply, mapf, reducef)
		}
		if reply.JobType == REDUCE_JOB {
			myReduce(&args, &reply, mapf, reducef)
		}
		if reply.JobType == WAIT {
			time.Sleep(300 * time.Millisecond)
			CallToCoordinator(mapf, reducef)
		}
		if reply.JobType == EXIT {
			os.Exit(1)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
}

func myMap(args *RequestArgs, reply *Reply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//fmt.Println(reply.ReduceNumber)
	Filename := reply.FileName
	file, _ := os.Open(Filename)
	Content, _ := ioutil.ReadAll(file)
	file.Close()
	intermediates := make(map[int][]KeyValue)
	allKva := mapf(Filename, string(Content))
	for _, kv := range allKva {
		reduceIdx := ihash(kv.Key) % reply.ReduceNumber
		intermediates[reduceIdx] = append(intermediates[reduceIdx], kv)
		//fmt.Println(intermediates[reduceIdx])
	}
	for i := 0; i < reply.ReduceNumber; i++ {
		f, err := os.CreateTemp(".", ("mr-" + strconv.Itoa(reply.MapIdx) + "-" + strconv.Itoa(i)))
		if err != nil {
			log.Fatal(err)
		}
		encoder := json.NewEncoder(f)
		//bucket := []KeyValue{KeyValue{"a", "1"}, KeyValue{"a", "1"}}
		sort.Sort(ByKey(intermediates[i]))
		//fmt.Printf("%v", bb)
		encoder.Encode(intermediates[i])
		//f.Write([]byte("dnjjsdjksjkj"))
		os.Rename(f.Name(), "./"+"mr-"+strconv.Itoa(reply.MapIdx)+"-"+strconv.Itoa(i))
		//fmt.Println("done")
	}

	args.JobDone = true
	args.MapIdx = reply.MapIdx
	args.JobType = MAP_JOB
	///////////
	reply.reset()
	//reply.MapIdx = 3
	callResult := call("Coordinator.WorkDistributor", args, reply)
	if callResult {
		//fmt.Println(reply)
		if reply.JobType == MAP_JOB {

			//fmt.Println("2got a map job!")
			myMap(args, reply, mapf, reducef)
		}
		if reply.JobType == REDUCE_JOB {
			//fmt.Println("got a reduce job!")
			myReduce(args, reply, mapf, reducef)
		}
		if reply.JobType == WAIT {
			time.Sleep(300 * time.Millisecond)
			CallToCoordinator(mapf, reducef)
		}
		if reply.JobType == EXIT {
			os.Exit(1)
		}
	}
}

func myReduce(args *RequestArgs, reply *Reply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//fmt.Println("rerrrrrr")
	reduceIdx := reply.ReduceIdx
	mapNumber := reply.MapNumber
	reduceRes := []KeyValue{}
	for i := 0; i < mapNumber; i++ {
		fileName := "./mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIdx)
		file, err := os.Open(fileName)
		if err != nil {
			//fmt.Println(err)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		content := []KeyValue{}
		err = decoder.Decode(&content)
		//fmt.Println(contentPointer)

		reduceRes = append(reduceRes, content...)
	}
	sort.Sort(ByKey(reduceRes))
	reduceFinal := []KeyValue{}
	if len(reduceRes) != 0 {
		CurrentKey := reduceRes[0].Key
		Values := []string{}
		for _, kv := range reduceRes {
			if kv.Key == CurrentKey {
				Values = append(Values, kv.Value)
			} else {
				newValue := reducef(CurrentKey, Values)
				reduceFinal = append(reduceFinal, KeyValue{CurrentKey, newValue})
				CurrentKey = kv.Key
				Values = []string{kv.Value}
			}
		}
		newValue := reducef(CurrentKey, Values)
		reduceFinal = append(reduceFinal, KeyValue{CurrentKey, newValue})
	}

	f, _ := os.CreateTemp(".", ("mr-out-" + strconv.Itoa(reply.ReduceIdx)))
	for _, kv := range reduceFinal {
		fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
	}
	os.Rename(f.Name(), "./mr-out-"+strconv.Itoa(reply.ReduceIdx))
	args.JobDone = true
	args.ReduceIdx = reply.ReduceIdx
	args.JobType = REDUCE_JOB
	reply.reset()
	callResult := call("Coordinator.WorkDistributor", args, reply)
	if callResult {
		if reply.JobType == MAP_JOB {
			//fmt.Println("3got a map job!")
			myMap(args, reply, mapf, reducef)
		}
		if reply.JobType == REDUCE_JOB {
			//fmt.Println("got a reduce job!")
			myReduce(args, reply, mapf, reducef)
		}
		if reply.JobType == WAIT {
			time.Sleep(300 * time.Millisecond)
			CallToCoordinator(mapf, reducef)
		}
		if reply.JobType == EXIT {
			os.Exit(1)
		}
	}

}

func call(rpcname string, args *RequestArgs, reply *Reply) bool {
	fmt.Println("asking")
	time.Sleep(300 * time.Millisecond)
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		fmt.Println(reply)
		return true
	}
	//fmt.Println(err)
	return false
}
