package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a *KeyValue) Equal(b *KeyValue) bool {
	return a.Key == b.Key && a.Value == b.Value
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		task, err := getTask()
		if err != nil {
			log.Fatalf("failed to get task: %v", err)
		}
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		switch task.Type {
		case MapTask:
			err := PerformMapTask(c, task, mapf)
			if err == context.DeadlineExceeded {
				reportTaskDone(task, FailedStatus)
				continue
			}
			if err != nil {
				reportTaskDone(task, FailedStatus)
				continue
			}
			reportTaskDone(task, DoneStatus)

		case ReduceTask:
			err := PerformReduceTask(c, task, reducef)
			if err == context.DeadlineExceeded {
				reportTaskDone(task, FailedStatus)
				continue
			}
			if err != nil {
				reportTaskDone(task, FailedStatus)
				continue
			}

			reportTaskDone(task, DoneStatus)
		case IdleTask:
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func reportTaskDone(task *Task, updatedStatus TaskStatus) {
	requestArg := NotifyTaskDoneRequest{
		Id:            task.Id,
		Type:          task.Type,
		UpdatedStatus: updatedStatus,
	}
	responseArg := TaskReply{
		Task: task,
	}
	call("Coordinator.MarkTaskComplete", &requestArg, &responseArg)
}

func getTask() (*Task, error) {

	req := struct{}{}
	var reply TaskReply

	ok := call("Coordinator.FetchTask", &req, &reply)
	if !ok {
		return &Task{Type: IdleTask}, fmt.Errorf("failed to fetch task")
	}
	return reply.Task, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func PerformMapTask(c context.Context, task *Task, mapf func(string, string) []KeyValue) error {

	content, err := os.ReadFile(task.File)
	if err != nil {
		return err
	}
	if err := c.Err(); err != nil {
		return err
	}
	// Call map function
	kva := mapf(task.File, string(content))
	// Write intermediate key-value pairs to files
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		if err := c.Err(); err != nil {
			return err
		}
		haskKey := ihash(kv.Key) % task.NReduce
		buckets[haskKey] = append(buckets[haskKey], kv)
	}

	for y, bucket := range buckets {
		if err := c.Err(); err != nil {
			return err
		}
		// create temp file
		fileName := fmt.Sprintf("mr-%d-%d", task.Id, y)
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			return err
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				return err
			}
		}
		tmpFile.Close()
		os.Rename(tmpFile.Name(), fileName)
	}
	return nil
}

func PerformReduceTask(c context.Context, task *Task, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	for m := 0; m < task.NMap; m++ {
		if err := c.Err(); err != nil {
			return err
		}
		fileName := fmt.Sprintf("mr-%d-%d", m, task.Id)
		fileContent, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("encountered error while opening file mr-%d-%d: %v\n", m, task.Id, err)
			continue
		}
		reader := json.NewDecoder(fileContent)
		for {
			if err := c.Err(); err != nil {
				fileContent.Close()
				return err
			}
			var kv KeyValue
			err := reader.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("error decoding json from %s: %v\n", fileName, err)
				break
			}
			intermediate = append(intermediate, kv)
		}
		fileContent.Close()
	}

	sort.Sort(ByKey(intermediate))

	// create output file
	outFileName := fmt.Sprintf("mr-out-%d", task.Id)
	outFile, err := os.Create(outFileName)
	if err != nil {
		return err
	}

	// Call reduce function and write to output file

	i := 0
	for i < len(intermediate) {
		if err := c.Err(); err != nil {
			outFile.Close()
			return err
		}
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outFile.Close()
	return nil
}
