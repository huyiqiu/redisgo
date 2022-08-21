package cluster

import "redisgo/interface/redis"

func execSelect(cluster *ClusterDatabase, c redis.Connection, cmdArgs [][]byte) redis.Reply {
	return cluster.db.Exec(c, cmdArgs)
}