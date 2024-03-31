package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Coordinator struct {
	// Your definitions here.
	nReduce int 
	mapLeft int 
	reduceLeft int 
	mu sync.Mutex
	mapTasks []*Task
	reduceTasks []*Task
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) HandleTask(args *TaskRequest, reply *TaskReply) error {
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
		task = &Task{TaskType: "Exit"}
	} else {
		task = &Task{TaskType: "None"}
	}
	// fmt.Println(task)
	reply.Task = task 
	return nil 
}

func (c *Coordinator) ReportTaskDone(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock() 
	defer c.mu.Unlock() 

	task := args.Task 
	if task.TaskType == "Map" && c.mapLeft > 0{
		task.State = "Completed"
		c.mapTasks[task.JobNumber] = task
		c.mapLeft--
	} else if task.TaskType == "Reduce" {
		task.State = "Completed" 
		c.reduceTasks[task.JobNumber] = task 
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
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]*Task, len(files))
	reduceTasks := make([]*Task, nReduce)

	for i := 0; i < len(files); i++ {
		mapTasks[i] = &Task{State: "Idle", JobNumber: i, TaskType: "Map", FName: files[i], NReduce: nReduce}
	}

	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = &Task{State: "Idle", JobNumber: i, TaskType: "Reduce", NReduce: nReduce}
	}

	c := Coordinator{nReduce: nReduce, mapLeft: len(files), reduceLeft: nReduce, mapTasks: mapTasks, reduceTasks: reduceTasks}

	c.server()
	return &c
}
