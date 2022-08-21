package cluster

import "redisgo/interface/redis"

type CmdLine = [][]byte

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)

	routerMap["ping"] = ping

	routerMap["del"] = Del

	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename

	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc

	routerMap["flushdb"] = FlushDB

	return routerMap
}

func defaultFunc(cluster *ClusterDatabase, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}