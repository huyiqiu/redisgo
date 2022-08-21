package cluster

import (
	"redisgo/interface/redis"
	"redisgo/redis/reply"
)

func Del(cluster *ClusterDatabase, c redis.Connection, args [][]byte) redis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("type errors")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}

	return reply.MakeErrReply("error occurs: " + errReply.Error())
}