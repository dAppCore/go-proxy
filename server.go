package proxy

import (
	"crypto/tls"
	"net"
)

// Server listens on one BindAddr and creates a Miner for each accepted connection.
//
//	srv, result := proxy.NewServer(
//	    proxy.BindAddr{Host: "0.0.0.0", Port: 3333, TLS: false},
//	    nil,
//	    proxy.NewRateLimiter(proxy.RateLimit{MaxConnectionsPerMinute: 30}),
//	    func(conn net.Conn, port uint16) { _ = conn; _ = port },
//	)
//	if result.OK {
//	    srv.Start()
//	}
type Server struct {
	addr      BindAddr
	tlsConfig *tls.Config // nil for plain TCP
	limiter   *RateLimiter
	onAccept  func(net.Conn, uint16)
	listener  net.Listener
	done      chan struct{}
}
