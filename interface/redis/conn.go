package redis

// redis 协议层
type Connection interface {
	Write([]byte) error // 给客户端回消息
	GetDBIndex() int    //查询客户端正在用的DB
	SelectDB(int)       //切换DB 函数
}
