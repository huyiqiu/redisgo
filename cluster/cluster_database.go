package cluster

import (
	"context"
	"fmt"
	"redisgo/config"
	database2 "redisgo/database"
	"redisgo/interface/database"
	"redisgo/interface/redis"
	"redisgo/lib/consistenthash"
	"redisgo/lib/logger"
	"redisgo/redis/reply"
	"runtime/debug"
	"strings"

	pool "github.com/jolestar/go-commons-pool/v2"
)


type ClusterDatabase struct {
	self string
	nodes []string
	peerPicker *consistenthash.NodeMap
	peerConnection map[string]*pool.ObjectPool // 连接池
	db database.Database
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *ClusterDatabase, c redis.Connection, cmdArgs [][]byte) redis.Reply

// 初始化一个cluster
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: config.Properties.Self,
		db: database2.NewStandaloneDataBase(),
		peerPicker: consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers) + 1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	// nodes = append(nodes, config.Properties.Peers...)
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...) //一致性哈希选择节点
	cluster.nodes = nodes
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	return cluster
}

var router = makeRouter()

func (c *ClusterDatabase) Exec(conn redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func ()  {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknowErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknow command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(c, conn, cmdLine)
	return 
}


func (c *ClusterDatabase) Close() {
	c.db.Close()
}


func (c *ClusterDatabase) AfterClientClose(conn redis.Connection) {
	c.db.AfterClientClose(conn)
}
