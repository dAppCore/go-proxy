package proxy

import (
	"sync"
	"time"
)

// Workers tracks per-identity aggregates derived from miner login fields.
//
// workers := proxy.NewWorkers(proxy.WorkersByRigID, bus)
// records := workers.List()
type Workers struct {
	mode       WorkersMode
	entries    []WorkerRecord // ordered by first-seen (stable)
	nameIndex  map[string]int // workerName → entries index
	idIndex    map[int64]int  // minerID → entries index
	subscribed bool
	mu         sync.RWMutex
}

// WorkerRecord is the aggregate row returned by Workers.List().
//
// record.Hashrate(60)
type WorkerRecord struct {
	Name        string
	LastIP      string
	Connections uint64
	Accepted    uint64
	Rejected    uint64
	Invalid     uint64
	Hashes      uint64 // sum of accepted share difficulties
	LastHashAt  time.Time
	windows     [5]tickWindow // 60s, 600s, 3600s, 12h, 24h
}
