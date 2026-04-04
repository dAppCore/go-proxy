package nicehash

import (
	"sync"

	"dappco.re/go/proxy"
)

// NonceStorage is the 256-slot fixed-byte allocation table for one NonceMapper.
//
// Slot encoding:
//
//	0           = free
//	+minerID    = active miner
//	-minerID    = disconnected miner (dead slot, cleared on next SetJob)
//
//	storage := nicehash.NewNonceStorage()
type NonceStorage struct {
	slots   [256]int64             // slot state per above encoding
	miners  map[int64]*proxy.Miner // minerID → Miner pointer for active miners
	job     proxy.Job              // current job from pool
	prevJob proxy.Job              // previous job (for stale submit validation)
	cursor  int                    // search starts here (round-robin allocation)
	mu      sync.Mutex
}
