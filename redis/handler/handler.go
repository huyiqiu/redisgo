package handler

import (
	"context"
	"io"
	"net"
	database2 "redisgo/database"
	"redisgo/interface/database"
	"redisgo/lib/logger"
	"redisgo/lib/sync/atomic"
	"redisgo/redis/connection"
	"redisgo/redis/parser"
	"redisgo/redis/reply"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.Database
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	var db database.Database
	// db = database2.NewEchoDatabase() // test
	db = database2.NewDataBase()
	return &Handler{db: db}
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

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			// io错误
			if payload.Err == io.ErrUnexpectedEOF ||
				payload.Err == io.EOF ||
				strings.Contains(payload.Err.Error(), "use of close network connection") {
				// close connection
				h.closeClient(client)
				logger.Info("connection closed:" + client.RemoteAddr().String())
				return
			}
			// 协议错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed:" + client.RemoteAddr().String())
				return
			}
			continue
		}

		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...") // 优雅退出
	h.closing.Set(true)
	h.activeConn.Range(func(key interface{}, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
