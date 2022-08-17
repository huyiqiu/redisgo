package database

import (
	"redisgo/interface/redis"
	"redisgo/redis/reply"
)

func Ping(db *DB, args [][]byte) redis.Reply {
	return &reply.PongReply{}
}

func init() {
	RegisterCommand("ping", Ping, 1)
}