// Package simple implements the passthrough splitter mode.
//
// Simple mode creates one upstream pool connection per miner. When ReuseTimeout > 0,
// the upstream connection is held idle for that many seconds after the miner disconnects,
// allowing the next miner to inherit it and avoid reconnect latency.
//
//	s := simple.NewSimpleSplitter(cfg, eventBus, strategyFactory)
package simple

import (
	"sync"

	"dappco.re/go/core/proxy"
	"dappco.re/go/core/proxy/pool"
)

// SimpleSplitter is the Splitter implementation for simple (passthrough) mode.
//
//	s := simple.NewSimpleSplitter(cfg, eventBus, strategyFactory)
type SimpleSplitter struct {
	active  map[int64]*SimpleMapper // minerID → mapper
	idle    map[int64]*SimpleMapper // mapperID → mapper (reuse pool, keyed by mapper seq)
	cfg     *proxy.Config
	events  *proxy.EventBus
	factory pool.StrategyFactory
	mu      sync.Mutex
	seq     int64 // monotonic mapper sequence counter
}
