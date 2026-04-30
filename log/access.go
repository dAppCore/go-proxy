// Package log implements append-only access and share logging for the proxy.
//
//	al, result := log.NewAccessLog("/var/log/proxy-access.log")
//	bus.Subscribe(proxy.EventLogin, al.OnLogin)
//	bus.Subscribe(proxy.EventClose, al.OnClose)
package log

import (
	"io"
	"sync"
)

// AccessLog writes connection lifecycle lines to an append-only text file.
//
// Line format (connect): 2026-04-04T12:00:00Z CONNECT  <ip>  <user>  <agent>
// Line format (close):   2026-04-04T12:00:00Z CLOSE    <ip>  <user>  rx=<bytes>  tx=<bytes>
//
//	al := log.NewAccessLog("/var/log/proxy-access.log")
//	bus.Subscribe(proxy.EventLogin, al.OnLogin)
//	bus.Subscribe(proxy.EventClose, al.OnClose)
type AccessLog struct {
	path string
	mu   sync.Mutex
	file io.WriteCloser
}
