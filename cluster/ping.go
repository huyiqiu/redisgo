package cluster

import "redisgo/interface/redis"

func ping(cluster *ClusterDatabase, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.db.Exec(c, args)
}