package proxy

// WorkerRow{"rig-alpha", "10.0.0.1", 1, 10, 0, 0, 100000, 1712232000, 1.0, 1.0, 1.0, 1.0, 1.0}
type WorkerRow [13]any

// MinerRow{1, "10.0.0.1:49152", 4096, 512, 2, 100000, "WALLET", "********", "rig-alpha", "XMRig/6.21.0"}
type MinerRow [10]any

// doc := p.SummaryDocument()
// doc.Results.Accepted == 4821
type SummaryDocument struct {
	Version         string                           `json:"version"`
	Mode            string                           `json:"mode"`
	Hashrate        HashrateDocument                 `json:"hashrate"`
	Miners          MinersCountDocument              `json:"miners"`
	Workers         uint64                           `json:"workers"`
	Upstreams       UpstreamDocument                 `json:"upstreams"`
	Results         ResultsDocument                  `json:"results"`
	CustomDiffStats map[uint64]CustomDiffBucketStats `json:"custom_diff_stats,omitempty"`
}

// HashrateDocument{Total: [6]float64{12345.67, 11900.00, 12100.00, 11800.00, 12000.00, 12200.00}}
type HashrateDocument struct {
	Total [6]float64 `json:"total"`
}

// MinersCountDocument{Now: 142, Max: 200}
type MinersCountDocument struct {
	Now uint64 `json:"now"`
	Max uint64 `json:"max"`
}

// UpstreamDocument{Active: 1, Sleep: 0, Error: 0, Total: 1, Ratio: 142.0}
type UpstreamDocument struct {
	Active uint64  `json:"active"`
	Sleep  uint64  `json:"sleep"`
	Error  uint64  `json:"error"`
	Total  uint64  `json:"total"`
	Ratio  float64 `json:"ratio"`
}

// ResultsDocument{Accepted: 4821, Rejected: 3, Invalid: 0, Expired: 12}
type ResultsDocument struct {
	Accepted    uint64     `json:"accepted"`
	Rejected    uint64     `json:"rejected"`
	Invalid     uint64     `json:"invalid"`
	Expired     uint64     `json:"expired"`
	AvgTime     uint32     `json:"avg_time"`
	Latency     uint32     `json:"latency"`
	HashesTotal uint64     `json:"hashes_total"`
	Best        [10]uint64 `json:"best"`
}

// doc := p.WorkersDocument()
// doc.Workers[0][0] == "rig-alpha"
type WorkersDocument struct {
	Mode    string      `json:"mode"`
	Workers []WorkerRow `json:"workers"`
}

// doc := p.MinersDocument()
// doc.Miners[0][7] == "********"
type MinersDocument struct {
	Format []string   `json:"format"`
	Miners []MinerRow `json:"miners"`
}
