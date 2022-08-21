package cluster

import (
	"redisgo/interface/redis"
	"redisgo/redis/reply"
)

func Rename(cluster *ClusterDatabase, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR cannot rename")
	}
	return cluster.relay(srcPeer, c, args)
}