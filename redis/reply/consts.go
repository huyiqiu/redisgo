package reply


// 回复Ping操作

type PongReply struct {
} 

var pongbytes = []byte("+PONG\r\n")

func (*PongReply) ToBytes() []byte {
	return pongbytes
}


func MakePongReply() *PongReply {
	return &PongReply{}
}

// OK
type OKReply struct{
}

var okbytes = []byte("+OK\r\n")

func (*OKReply) ToBytes() []byte{
	return okbytes
}


func MakeOkReply() *OKReply {
	return &OKReply{}
}

// NULLBulk
type NullBulkReply struct {
}

var nullbytes = []byte("$-1\r\n")

func (*NullBulkReply) ToBytes() []byte {
	return nullbytes
}


func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// Emptymultibulk
type EmptyMultiBulkReply struct {
}

var emptyMultiBulkbytes = []byte("*0\r\n")

func (*EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkbytes
}

func MakeEmptyMultiBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// no reply
type NoReply struct {
}

var nobytes = []byte("")

func (*NoReply) ToBytes() []byte {
	return nobytes
}
