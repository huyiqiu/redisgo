package cluster

import (
	"redisgo/interface/redis"
	"redisgo/redis/reply"
)

func FlushDB(cluster *ClusterDatabase, c redis.Connection, args [][]byte) redis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OKReply{}
	}
	return reply.MakeErrReply("errors occurs: " + errReply.Error())
}