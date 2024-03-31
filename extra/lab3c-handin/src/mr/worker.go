package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const fileCode = 0755
const fileCommand = os.O_RDWR|os.O_CREATE

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func applyTaskDone(t *Task) {
	args := FinishTaskArgs{Task: t}
	reply := FinishTaskReply{}
	if ok := call("Coordinator.ApplyTaskDone", &args, &reply); !ok {
			os.Exit(0) 
	}
}

func applyReduce(t *Task, reducef func(string, []string) string) {
	files, err := filepath.Glob(fmt.Sprintf("mr-%v-%v.json", "*", t.JobNum))
	if err != nil {
		log.Fatalf("cannot find files for reduce job")
	}
	
	kvmap := make(map[string][]string)
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("cannot open file, %v", f)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break 
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}
	allKeys := make([]string, len(kvmap))
	i := 0 
	for key := range kvmap {
		allKeys[i] = key 
		i++
	}

	sort.Strings(allKeys)

	tempname := fmt.Sprintf("temp-mr-out-%v.json", t.JobNum)
	tempfile, err := os.OpenFile(tempname, fileCommand, fileCode)
	if err != nil {
		return
	}
	defer os.Remove(tempname)

	for _, key := range allKeys {
		value := reducef(key, kvmap[key])
		_, err = fmt.Fprintf(tempfile, "%v %v\n", key, value)
		if err != nil {
			log.Fatalf("cannot write reduce values: %v-%v", key, value)
		}
	}
	newname := fmt.Sprintf("mr-out-%v.json", t.JobNum)
	if err := os.Rename(tempname, newname); err != nil {
		return
	}
	applyTaskDone(t)
}

func applyMap(t *Task, mapf func(string, string) []KeyValue) {
	filename := t.FName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	
	temp := mapf(filename, string(content))
	kvArr := make([][]KeyValue, t.NumReduce)
	for _, kv := range temp {
		idx := ihash(kv.Key) % t.NumReduce
		kvArr[idx] = append(kvArr[idx], kv)
	}

	for i, elem := range kvArr {
		// name in format mr-X-Y where X is map number and Y is reduce number
		tempname := fmt.Sprintf("temp-mr-%v-%v.json", t.JobNum, i)
		tempfile, err := os.OpenFile(tempname, fileCommand, fileCode)
		if err != nil {
			return
		}
		defer os.Remove(tempname)

		enc := json.NewEncoder(tempfile)
		for _, kv := range elem {
			if err := enc.Encode(&kv); err != nil {
				return 
			}
		}

		newname := fmt.Sprintf("mr-%v-%v.json", t.JobNum, i)
		if err := os.Rename(tempname, newname); err != nil {
			return
		}
	}
	applyTaskDone(t) 
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskRequest{ WorkerId: os.Getpid() }
		reply := TaskReply{}
		if ok := call("Coordinator.ApplyTask", &args, &reply); !ok {
			os.Exit(0) 
		}
		if reply.Task == nil {
			// task is nil for some reason 
		} else if reply.Task.TaskType == "Map" {
			applyMap(reply.Task, mapf)
		} else if reply.Task.TaskType == "Reduce" {
			applyReduce(reply.Task, reducef)
		} else if reply.Task.TaskType == "Finish" {
			os.Exit(0) 
		}
		time.Sleep(time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
