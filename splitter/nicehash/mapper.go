package nicehash

import (
	"sync"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

// NonceMapper manages one outbound pool connection and the 256-slot NonceStorage.
// It implements pool.StratumListener to receive job and result events from the pool.
//
//	m := nicehash.NewNonceMapper(id, cfg, strategy)
//	m.Start()
type NonceMapper struct {
	id        int64
	storage   *NonceStorage
	strategy  pool.Strategy           // manages pool client lifecycle and failover
	pending   map[int64]SubmitContext // sequence → {requestID, minerID}
	cfg       *proxy.Config
	events    *proxy.EventBus
	active    bool // true once pool has sent at least one job
	suspended int  // > 0 when pool connection is in error/reconnecting
	lastUsed  time.Time
	mu        sync.Mutex
}

// SubmitContext tracks one in-flight share submission waiting for pool reply.
//
//	ctx := SubmitContext{RequestID: 42, MinerID: 7}
type SubmitContext struct {
	RequestID int64 // JSON-RPC id from the miner's submit request
	MinerID   int64 // miner that submitted
	JobID     string
}
