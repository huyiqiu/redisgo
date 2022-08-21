package cluster

import (
	"context"
	"errors"
	"redisgo/interface/redis"
	"redisgo/lib/utils"
	"redisgo/redis/client"
	"redisgo/redis/reply"
	"strconv"
)

// borrow object from connection pool
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

// return object to the connection pool
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return factory.ReturnObject(context.Background(), peerClient)
}

// relay relays command to peer
func (cluster *ClusterDatabase) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	} ()
	peerClient.Send(utils.ToCmdLine("select", strconv.Itoa(c.GetDBIndex())))
	reply := peerClient.Send(args)
	return reply
}


// broadcast broadvcasts command to all nodes in cluster
func (cluster *ClusterDatabase) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.nodes { //挨个node执行
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}