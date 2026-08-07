package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskReply struct {
	Task *Task
}

// Add your RPC definitions here.

type Phase int
type TaskType int

var MapPhase Phase = 1
var ReducePhase Phase = 2
var DonePhase Phase = 3

var IdleTask TaskType = 0
var MapTask TaskType = 1
var ReduceTask TaskType = 2

type TaskStatus int

const (
	IdleStatus TaskStatus = iota
	RunningStatus
	DoneStatus
	FailedStatus
)

type Task struct {
	Type    TaskType
	Id      int
	File    string
	Status  TaskStatus
	NReduce int
}

type NotifyTaskDoneRequest struct {
	Id   int
	Type TaskType
}
