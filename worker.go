package proxy

import (
	"sync"
	"time"
)

// Workers tracks per-worker aggregates derived from miner login fields.
//
//	workers := proxy.NewWorkers(proxy.WorkersByRigID, bus)
//	workers.OnLogin(proxy.Event{Miner: &proxy.Miner{rigID: "rig-alpha", user: "WALLET", ip: "10.0.0.1"}})
//	_ = workers.List()
type Workers struct {
	mode       WorkersMode
	entries    []WorkerRecord // ordered by first-seen (stable)
	nameIndex  map[string]int // workerName → entries index
	idIndex    map[int64]int  // minerID → entries index
	subscribed bool
	mu         sync.RWMutex
}

// WorkerRecord is the aggregate row returned by `Workers.List()`.
//
//	record := proxy.WorkerRecord{Name: "rig-alpha"}
//	_ = record.Hashrate(60)
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
