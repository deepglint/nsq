package protocol

import (
	"fmt"
	"net"

	"github.com/deepglint/nsq/internal/app"
)

type UDPHandler interface {
	Handle(*net.UDPConn)
}

func UDPServer(conn **net.UDPConn, handler UDPHandler, l app.Logger) {
	l.Output(2, fmt.Sprintf("UDP: listening on %s", (*conn).LocalAddr()))

	for {
		if *conn == nil {
			return
		}
		handler.Handle(*conn)
	}
}
