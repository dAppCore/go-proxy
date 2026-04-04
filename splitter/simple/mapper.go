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
	strategy   pool.Strategy
	idleAt     time.Time // zero when active
	stopped    bool
	events     *proxy.EventBus
	pending    map[int64]*proxy.SubmitEvent
	mu         sync.Mutex
}
