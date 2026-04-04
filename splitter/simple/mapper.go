package simple

import (
	"sync"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

// SimpleMapper holds one outbound pool connection and serves at most one active miner
// at a time. It becomes idle when the miner disconnects and may be reclaimed for the
// next login.
//
//	m := simple.NewSimpleMapper(id, strategy)
type SimpleMapper struct {
	id         int64
	miner      *proxy.Miner // nil when idle
	currentJob proxy.Job
	prevJob    proxy.Job
	strategy   pool.Strategy
	idleAt     time.Time // zero when active
	stopped    bool
	events     *proxy.EventBus
	pending    map[int64]submitContext
	mu         sync.Mutex
}

type submitContext struct {
	RequestID int64
	StartedAt time.Time
	JobID     string
}

// NewSimpleMapper creates a passthrough mapper for one pool connection.
//
//	m := simple.NewSimpleMapper(7, strategy)
func NewSimpleMapper(id int64, strategy pool.Strategy) *SimpleMapper {
	return &SimpleMapper{
		id:       id,
		strategy: strategy,
		pending:  make(map[int64]submitContext),
	}
}
