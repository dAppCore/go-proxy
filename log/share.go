package log

import (
	"os"
	"sync"
)

// ShareLog writes share result lines to an append-only text file.
//
// Line format (accept): 2026-04-04T12:00:00Z ACCEPT  <user>  diff=<diff>  latency=<ms>ms
// Line format (reject): 2026-04-04T12:00:00Z REJECT  <user>  reason="<message>"
//
//	sl := log.NewShareLog("/var/log/proxy-shares.log")
//	bus.Subscribe(proxy.EventAccept, sl.OnAccept)
//	bus.Subscribe(proxy.EventReject, sl.OnReject)
type ShareLog struct {
	path string
	mu   sync.Mutex
	f    *os.File
}
