package proxy

import (
	"crypto/tls"
	"net"
)

// Server listens on one BindAddr and creates a Miner for each accepted connection.
//
//	srv, result := proxy.NewServer(bind, tlsCfg, rateLimiter, onAccept)
//	srv.Start()
type Server struct {
	addr     BindAddr
	tlsCfg   *tls.Config // nil for plain TCP
	limiter  *RateLimiter
	onAccept func(net.Conn, uint16)
	listener net.Listener
	done     chan struct{}
}
