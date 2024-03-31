package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	WorkerId int 
	State string // Idle, Progress, Completed
	JobNumber int
	FName string // for Map jobs 
	NReduce int 
	TaskType string // "Exit", "None", "Map", "Reduce"
	Timestamp time.Time
}

// Add your RPC definitions here.
type TaskRequest struct {
	WorkerId int 
}

type TaskReply struct {
	Task *Task 
}

type FinishTaskArgs struct {
	Task *Task
}

type FinishTaskReply struct {
	Success bool 
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
