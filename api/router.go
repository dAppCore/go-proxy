// Package api implements the HTTP monitoring endpoints for the proxy.
//
// Registered routes:
//
//	GET /1/summary — aggregated proxy stats
//	GET /1/workers — per-worker hashrate table
//	GET /1/miners  — per-connection state table
//
//	proxyapi.RegisterRoutes(apiRouter, p)
package api

// SummaryResponse is the /1/summary JSON body.
//
//	{"version":"1.0.0","mode":"nicehash","hashrate":{"total":[...]}, ...}
type SummaryResponse struct {
	Version   string              `json:"version"`
	Mode      string              `json:"mode"`
	Hashrate  HashrateResponse    `json:"hashrate"`
	Miners    MinersCountResponse `json:"miners"`
	Workers   uint64              `json:"workers"`
	Upstreams UpstreamResponse    `json:"upstreams"`
	Results   ResultsResponse     `json:"results"`
}

// HashrateResponse carries the per-window hashrate array.
//
//	HashrateResponse{Total: [6]float64{12345.67, 11900.00, 12100.00, 11800.00, 12000.00, 12200.00}}
type HashrateResponse struct {
	Total [6]float64 `json:"total"`
}

// MinersCountResponse carries current and peak miner counts.
//
//	MinersCountResponse{Now: 142, Max: 200}
type MinersCountResponse struct {
	Now uint64 `json:"now"`
	Max uint64 `json:"max"`
}

// UpstreamResponse carries pool connection state counts.
//
//	UpstreamResponse{Active: 1, Sleep: 0, Error: 0, Total: 1, Ratio: 142.0}
type UpstreamResponse struct {
	Active uint64  `json:"active"`
	Sleep  uint64  `json:"sleep"`
	Error  uint64  `json:"error"`
	Total  uint64  `json:"total"`
	Ratio  float64 `json:"ratio"`
}

// ResultsResponse carries share acceptance statistics.
//
//	ResultsResponse{Accepted: 4821, Rejected: 3, Invalid: 0, Expired: 12}
type ResultsResponse struct {
	Accepted    uint64     `json:"accepted"`
	Rejected    uint64     `json:"rejected"`
	Invalid     uint64     `json:"invalid"`
	Expired     uint64     `json:"expired"`
	AvgTime     uint32     `json:"avg_time"`
	Latency     uint32     `json:"latency"`
	HashesTotal uint64     `json:"hashes_total"`
	Best        [10]uint64 `json:"best"`
}
