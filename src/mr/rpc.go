package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.
const MAP_JOB = 1
const REDUCE_JOB = 0
const WAIT = 2
const EXIT = 3

type RequestArgs struct {
	NeedJob   bool
	JobDone   bool
	MapIdx    int
	ReduceIdx int
	JobType   int
}

type Reply struct {
	MapNumber    int
	MapIdx       int
	JobType      int
	FileName     string
	ReduceNumber int
	ReduceIdx    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	//s := "/Users/shiyanpei/Desktop/tmp/824-mr"
	//s += strconv.Itoa(os.Getuid())
	s := "/Users/shiyanpei/Desktop/tmp/3"
	return s
}
