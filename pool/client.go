// Package pool implements the outbound stratum pool client and failover strategy.
//
//	client := pool.NewStratumClient(poolCfg, listener)
//	client.Connect()
package pool

import (
	"crypto/tls"
	"net"
	"sync"

	"dappco.re/go/proxy"
)

// client := NewStratumClient(poolCfg, listener)
// client.Connect()
type StratumClient struct {
	config     proxy.PoolConfig
	listener   StratumListener
	conn       net.Conn
	tlsConn    *tls.Conn // nil if plain TCP
	sessionID  string    // pool-assigned session id from login reply
	seq        int64     // atomic JSON-RPC request id counter
	active     bool      // true once first job received
	pending    map[int64]struct{}
	closedOnce sync.Once
	mu         sync.Mutex
	sendMu     sync.Mutex
}

// type listener struct{}
//
// func (listener) OnJob(job proxy.Job) {}
type StratumListener interface {
	// OnJob is called when the pool pushes a new job notification or the login reply contains a job.
	OnJob(job proxy.Job)
	// OnResultAccepted is called when the pool accepts or rejects a submitted share.
	// sequence matches the value returned by Submit(). errorMessage is "" on accept.
	OnResultAccepted(sequence int64, accepted bool, errorMessage string)
	// OnDisconnect is called when the pool TCP connection closes for any reason.
	OnDisconnect()
}
