package reply

// 未知错误
type UnknowErrReply struct {
}

var unknownErrBytes = []byte("-Err 未知错误\r\n")

func (*UnknowErrReply) Error() string {
	return "未知错误"
}

func (*UnknowErrReply) ToBytes() []byte {
	return unknownErrBytes
}

//参数数量错误
type ArgNumErrReply struct {
	Cmd string
}

func (a *ArgNumErrReply) Error() string {
	return "-ERR 命令 '" + a.Cmd + "' 参数数量错误"
}

func (a *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR 命令'" + a.Cmd + "'参数数量错误\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply{
	return &ArgNumErrReply{Cmd: cmd}
}


// 语法错误
type SyntaxErrReply struct{
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return []byte("-Err 语法错误\r\n")
}

func (r *SyntaxErrReply) Error() string {
	return "Err 语法错误"
}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return &SyntaxErrReply{}
}

//类型错误
type WrongTypeErrReply struct{
}

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return []byte("-WRONGTYPE 对持有错误类型值的键进行操作\r\n")
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE 对持有错误类型值的键进行操作"
}

// RESP协议错误
type ProtocolErrReply struct {
	Msg string
}

// ToBytes marshals redis.Reply
func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR 协议错误: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR 协议错误: '" + r.Msg
}