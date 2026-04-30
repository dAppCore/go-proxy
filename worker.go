package proxy

import (
	"sync"
	"time"
)

// Workers maintains per-worker aggregate stats. Workers are identified by name,
// derived from the miner's login fields per WorkersMode.
//
//	workers := proxy.NewWorkers(proxy.WorkersByRigID, bus)
//	workers.OnLogin(proxy.Event{Miner: miner})
//	records := workers.List()
type Workers struct {
	mode       WorkersMode
	entries    []WorkerRecord // ordered by first-seen (stable)
	nameIndex  map[string]int // workerName → entries index
	idIndex    map[int64]int  // minerID → entries index
	subscribed bool
	mu         sync.RWMutex
}

// WorkerRecord is the per-identity aggregate with rolling hashrate windows.
//
//	record := proxy.WorkerRecord{Name: "rig-alpha", Accepted: 10, Hashes: 500000}
//	hr60 := record.Hashrate(60) // H/s over the last 60 seconds
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
