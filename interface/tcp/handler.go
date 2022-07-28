package tcp

import (
	"context"
	"net"
)

// 处理程序通过tcp表示应用服务器
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}