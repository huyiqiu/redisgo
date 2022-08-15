package database

import "redisgo/interface/redis"



type CmdLine = [][]byte



type Database interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply
	Close()
	AfterClientClose(c redis.Connection)
}

type DataEntity struct {
	Data interface{}
}