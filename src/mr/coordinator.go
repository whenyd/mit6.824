package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	InputFiles         []string
	MapIndex           int
	IntermediateFiles  []string
	ReduceIndex        int
	nReduce            int
	ReduceWorkerStatus string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if len(c.InputFiles) == 0 {
			reply.Err = ErrNoFile
			return nil
		}

		if c.MapIndex == len(c.InputFiles)-1 {
			// reply.FileName = ""
			reply.Err = ErrNoTask
			return nil
		}

		reply.FileName = c.InputFiles[c.MapIndex]
		// reply.Err = ""
		c.MapIndex++
		return nil
	} else if args.TaskType == ReduceTask {
		if len(c.IntermediateFiles) == 0 {
			reply.Err = ErrNoFile
			return nil
		}

		if c.ReduceIndex == len(c.IntermediateFiles)-1 {
			reply.Err = ErrNoTask
			return nil
		}

		reply.FileName = c.IntermediateFiles[c.ReduceIndex]
		c.ReduceIndex++
		return nil
	} else {
		// reply.FileName = ""
		reply.Err = ErrUnsupportedTask
		return nil
	}
}

func (c *Coordinator) PutTask(args *PutTaskArgs, reply *PutTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.IntermediateFiles = append(c.IntermediateFiles, args.IntermediateFileName)
	reply.Err = OK
	return nil
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

//
// start a thread that listens for RPCs from worker.go
//
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
	// c := Coordinator{}

	// Your code here.
	c := new(Coordinator)
	c.InputFiles = files
	c.nReduce = nReduce

	c.server()
	return c
}
