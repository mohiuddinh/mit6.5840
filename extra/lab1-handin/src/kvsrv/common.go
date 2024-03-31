package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	UUID int
	ClientID int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	UUID int 
	ClientID int64
}

type GetReply struct {
	Value string
}
