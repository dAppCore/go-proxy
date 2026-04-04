// Package nicehash implements the NiceHash nonce-splitting mode.
//
// It partitions the 32-bit nonce space among miners by fixing one byte (byte 39
// of the blob). Each upstream pool connection (NonceMapper) owns a 256-slot table.
// Up to 256 miners share one pool connection. The 257th miner triggers creation
// of a new NonceMapper with a new pool connection.
//
//	s := nicehash.NewNonceSplitter(cfg, eventBus, strategyFactory)
//	s.Connect()
package nicehash

import (
	"sync"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

// NonceSplitter is the Splitter implementation for NiceHash mode.
// It manages a dynamic slice of NonceMapper upstreams, creating new ones on demand.
//
//	s := nicehash.NewNonceSplitter(cfg, eventBus, strategyFactory)
//	s.Connect()
type NonceSplitter struct {
	mappers         []*NonceMapper
	byID            map[int64]*NonceMapper
	cfg             *proxy.Config
	events          *proxy.EventBus
	strategyFactory pool.StrategyFactory
	mu              sync.RWMutex
	seq             int64
}
