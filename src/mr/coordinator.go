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

const DONE = 2
const PENDING = 1
const NOT_ASSIGNED = 0

type WorkStatus struct {
	Status []int
	mu     sync.Mutex //-------------?
}
type Coordinator struct {

	// Your definitions here.
	fileNameList     []string
	fileName         string
	nReduce          int
	mapWorkStatus    WorkStatus
	reduceWorkStatus WorkStatus
}

func (c *Coordinator) checkForCrash(jobType int, jobIdx int) {
	fmt.Println("here")
	time.Sleep(10 * time.Second)
	fmt.Println("still here")
	if jobType == MAP_JOB {
		c.mapWorkStatus.mu.Lock()
		if c.mapWorkStatus.Status[jobIdx] != DONE {
			c.mapWorkStatus.Status[jobIdx] = NOT_ASSIGNED
		}
		c.mapWorkStatus.mu.Unlock()
	}

	if jobType == REDUCE_JOB {
		c.reduceWorkStatus.mu.Lock()
		if c.reduceWorkStatus.Status[jobIdx] != DONE {
			c.reduceWorkStatus.Status[jobIdx] = NOT_ASSIGNED
		}
		c.reduceWorkStatus.mu.Unlock()
	}

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) WorkDistributor(args *RequestArgs, reply *Reply) error {
	//Println("got request", args)
	if c.Done() {
		reply.JobType = EXIT
		return nil
	}
	reply.ReduceNumber = c.nReduce
	reply.MapNumber = len(c.fileNameList)
	//time.Sleep(5 * time.Second)

	if args.JobDone {
		if args.JobType == MAP_JOB {
			c.mapWorkStatus.mu.Lock()
			if c.mapWorkStatus.Status[args.MapIdx] != DONE {
				c.mapWorkStatus.Status[args.MapIdx] = DONE
			}
			c.mapWorkStatus.mu.Unlock()
		} else if args.JobType == REDUCE_JOB {
			c.reduceWorkStatus.mu.Lock()
			if c.reduceWorkStatus.Status[args.ReduceIdx] != DONE {
				c.reduceWorkStatus.Status[args.ReduceIdx] = DONE
			}
			c.reduceWorkStatus.mu.Unlock()
		}
	}
	if args.NeedJob {
		//fmt.Println("here")
		assigned := false
		MapsDone := true
		c.mapWorkStatus.mu.Lock()

		for i, status := range c.mapWorkStatus.Status {
			if status == NOT_ASSIGNED {
				c.mapWorkStatus.Status[i] = PENDING
				reply.JobType = MAP_JOB
				reply.MapIdx = i
				fileName := c.fileNameList[i]
				reply.FileName = fileName
				go c.checkForCrash(MAP_JOB, i)
				//fmt.Println(reply)
				//return nil
				assigned = true
				//fmt.Println("assigned map")
				break
			}
			if status != DONE {
				MapsDone = false
			}
		}
		c.mapWorkStatus.mu.Unlock()
		if !assigned && MapsDone {
			c.reduceWorkStatus.mu.Lock()
			//fmt.Println("in reduce")
			for i, status := range c.reduceWorkStatus.Status {
				if status == NOT_ASSIGNED {
					//fmt.Println("1")
					c.reduceWorkStatus.Status[i] = PENDING
					//fmt.Println("2")
					//reply.FileName = "bbb"
					reply.JobType = REDUCE_JOB
					go c.checkForCrash(REDUCE_JOB, i)
					//reply.MapNumber = len(c.fileNameList)
					//fmt.Println("3")
					reply.ReduceIdx = i
					//reply.MapIdx = 10
					//fmt.Println("4")
					assigned = true
					//fmt.Println("5")
					//fmt.Println("assigned reduce")
					break
				}
			}
			//fmt.Println("6")
			c.reduceWorkStatus.mu.Unlock()
		}
		if !assigned {
			reply.JobType = WAIT
		}
	}
	//fmt.Println(reply)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	res := true
	c.mapWorkStatus.mu.Lock()
	for _, status := range c.mapWorkStatus.Status {
		if status != DONE {
			res = false
		}
	}
	fmt.Println(c.mapWorkStatus.Status)
	c.mapWorkStatus.mu.Unlock()
	c.reduceWorkStatus.mu.Lock()
	for _, status := range c.reduceWorkStatus.Status {
		if status != DONE {
			res = false
		}
	}
	fmt.Println(c.reduceWorkStatus.Status)
	c.reduceWorkStatus.mu.Unlock()
	return res
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := Coordinator{}
	coordinator.fileNameList = files

	coordinator.nReduce = nReduce
	for i := 0; i < len(files); i++ {
		coordinator.mapWorkStatus.Status = append(coordinator.mapWorkStatus.Status, NOT_ASSIGNED)
	}
	for i := 0; i < nReduce; i++ {
		coordinator.reduceWorkStatus.Status = append(coordinator.reduceWorkStatus.Status, NOT_ASSIGNED)
	}

	coordinator.server()
	return &coordinator
}
