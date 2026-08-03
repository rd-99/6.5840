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
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	phase       Phase
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	c := Coordinator{
		mu:          sync.Mutex{},
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       MapPhase,
	}

	// Your code here.

	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:    MapTask,
			Id:      i,
			File:    file,
			Status:  IdleStatus,
			nReduce: 10,
		}
	}

	c.server(sockname)
	return &c
}

func (c *Coordinator) fetchTask(args *struct{}, reply *TaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		for i, task := range c.mapTasks {
			if task.Status == IdleStatus {
				c.mapTasks[i].Status = RunningStatus
				reply.task = &c.mapTasks[i]
				return nil
			}

			if task.Status == FailedStatus {
				c.mapTasks[i].Status = RunningStatus
				reply.task = &c.mapTasks[i]
				return nil
			}
		}

	case ReducePhase:
		for i, task := range c.reduceTasks {
			if task.Status == IdleStatus {
				c.reduceTasks[i].Status = RunningStatus
				reply.task = &c.reduceTasks[i]
				return nil
			}

			if task.Status == FailedStatus {
				c.reduceTasks[i].Status = RunningStatus
				reply.task = &c.reduceTasks[i]
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) MarkTaskComplete(args *NotifyTaskDoneRequest, reply *TaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case MapTask:
		c.mapTasks[args.Id].Status = DoneStatus
		if c.checkAllMapTasksDone() {
			c.phase = ReducePhase
		}
	case ReduceTask:
		c.reduceTasks[args.Id].Status = DoneStatus
		if c.checkAllReduceTasksDone() {
			c.phase = DonePhase
		}
	}
	return nil
}

func (c *Coordinator) checkAllMapTasksDone() bool {
	for _, task := range c.mapTasks {
		if task.Status != DoneStatus {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkAllReduceTasksDone() bool {
	for _, task := range c.reduceTasks {
		if task.Status != DoneStatus {
			return false
		}
	}
	return true
}
