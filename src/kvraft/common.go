package kvraft

const (
	OK             = "OK"
	ErrNotApplied  = "ErrNotApplied"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNoKey       = "ErrNoKey"

	GET = "GET"
	PUT = "PUT"
	APPEND = "APPEND"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	CommandId int64
	ClerkId int64
	Type string
}

type PutAppendReply struct {
	Value string
	Status string
}

type GetArgs struct {
	Key string
	CommandId int64 
	ClerkId int64
}

type GetReply struct {
	Value string
	Status string
}

