package database

import (
	"redisgo/interface/redis"
	"redisgo/redis/reply"
)

// test
// 发什么回什么
type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e *EchoDatabase) Exec(client redis.Connection, args [][]byte) redis.Reply {
	return reply.MakeMultiBulkReply(args)
}

func (e *EchoDatabase) Close() {

}

func (e *EchoDatabase) AfterClientClose(c redis.Connection) {

}