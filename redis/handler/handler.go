package handler

import (
	"context"
	"net"
	"redisgo/interface/database"
	"redisgo/lib/logger"
	"redisgo/lib/sync/atomic"
	"redisgo/redis/connection"
	"sync"
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.Database
	closing    atomic.Boolean // refusing new client and new request
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key interface{}, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
