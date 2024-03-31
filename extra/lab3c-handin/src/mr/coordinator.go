package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	numReduce int 
	mapLeft int 
	reduceLeft int 
	mu sync.Mutex
	mapTasks []*Task
	reduceTasks []*Task
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) ApplyTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock() 
	defer c.mu.Unlock() 

	var task *Task
	if c.mapLeft > 0 {
		// assign first "free" task to worker 
		for i, t := range c.mapTasks {
			if t.State == "Idle" || (t.State == "Progress" && time.Since(t.Timestamp) > time.Second*10) {
				task = c.mapTasks[i]
				task.WorkerId = args.WorkerId 
				task.Timestamp = time.Now() 
				task.State = "Progress"
				break 
			}
		}
	} else if c.reduceLeft > 0 {
		for i, t := range c.reduceTasks {
			if t.State == "Idle" || (t.State == "Progress" && time.Since(t.Timestamp) > time.Second*10) {
				task = c.reduceTasks[i]
				task.WorkerId = args.WorkerId 
				task.Timestamp = time.Now() 
				task.State = "Progress"
				break 
			}
		}
	} else if c.mapLeft == 0 && c.reduceLeft == 0{
		// terminate
		task = &Task{TaskType: "Finish"}
	} else {
		task = &Task{TaskType: "None"}
	}
	// fmt.Println(task)
	reply.Task = task 
	return nil 
}

func (c *Coordinator) ApplyTaskDone(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock() 
	defer c.mu.Unlock() 

	task := args.Task 
	if task.TaskType == "Map" && c.mapLeft > 0{
		task.State = "Completed"
		c.mapTasks[task.JobNum] = task
		c.mapLeft--
	} else if task.TaskType == "Reduce" {
		task.State = "Completed" 
		c.reduceTasks[task.JobNum] = task 
		c.reduceLeft--
	}
	reply.Success = true 
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
	c.mu.Lock() 
	defer c.mu.Unlock() 

	return c.mapLeft == 0 && c.reduceLeft == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// numReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, numReduce int) *Coordinator {
	mapTasks := make([]*Task, len(files))
	reduceTasks := make([]*Task, numReduce)

	for i := 0; i < len(files); i++ {
		mapTasks[i] = &Task{State: "Idle", JobNum: i, TaskType: "Map", FName: files[i], NumReduce: numReduce}
	}

	for i := 0; i < numReduce; i++ {
		reduceTasks[i] = &Task{State: "Idle", JobNum: i, TaskType: "Reduce", NumReduce: numReduce}
	}

	c := Coordinator{numReduce: numReduce, mapLeft: len(files), reduceLeft: numReduce, mapTasks: mapTasks, reduceTasks: reduceTasks}

	c.server()
	return &c
}
