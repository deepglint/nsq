package http_api

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/deepglint/nsq/internal/app"
)

func Serve(listener net.Listener, handler http.Handler, proto string, l app.Logger) {
	l.Output(2, fmt.Sprintf("%s: listening on %s", proto, listener.Addr()))

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		l.Output(2, fmt.Sprintf("ERROR: http.Serve() - %s", err))
	}

	l.Output(2, fmt.Sprintf("%s: closing %s", proto, listener.Addr()))
}
