package proxy

const (
	// MonitoringRouteSummary documents the summary endpoint path.
	//
	//	http.Get("http://127.0.0.1:8080" + proxy.MonitoringRouteSummary)
	MonitoringRouteSummary = "/1/summary"

	// MonitoringRouteWorkers documents the workers endpoint path.
	//
	//	http.Get("http://127.0.0.1:8080" + proxy.MonitoringRouteWorkers)
	MonitoringRouteWorkers = "/1/workers"

	// MonitoringRouteMiners documents the miners endpoint path.
	//
	//	http.Get("http://127.0.0.1:8080" + proxy.MonitoringRouteMiners)
	MonitoringRouteMiners = "/1/miners"

	// SummaryDocumentVersion is the monitoring API version.
	//
	//	doc := proxy.SummaryDocument{Version: proxy.SummaryDocumentVersion}
	SummaryDocumentVersion = "1.0.0"
)

var (
	// MinersDocumentFormat defines the fixed /1/miners column order.
	//
	//	doc := proxy.MinersDocument{Format: append([]string(nil), proxy.MinersDocumentFormat...)}
	MinersDocumentFormat = []string{"id", "ip", "tx", "rx", "state", "diff", "user", "password", "rig_id", "agent"}

	workerHashrateWindows = [5]int{60, 600, 3600, 43200, 86400}
)

// WorkerRow{"rig-alpha", "10.0.0.1", 1, 10, 0, 0, 10000, 1712232000, 1.0, 1.0, 1.0, 1.0, 1.0}
type WorkerRow [13]any

// MinerRow{1, "10.0.0.1:49152", 4096, 512, 2, 10000, "WALLET", maskedPassword, "rig-alpha", "XMRig/6.21.0"}
type MinerRow [10]any

// doc := p.SummaryDocument()
// _ = doc.Results.Accepted
// _ = doc.Upstreams.Ratio
type SummaryDocument struct {
	Version   string              `json:"version"`
	Mode      string              `json:"mode"`
	Hashrate  HashrateDocument    `json:"hashrate"`
	Miners    MinersCountDocument `json:"miners"`
	Workers   uint64              `json:"workers"`
	Upstreams UpstreamDocument    `json:"upstreams"`
	Results   ResultsDocument     `json:"results"`
}

// SummaryResponse is the RFC name for SummaryDocument.
type SummaryResponse = SummaryDocument

// HashrateDocument{Total: [6]float64{12345.67, 11900.00, 12100.00, 11800.00, 12000.00, 12200.00}}
type HashrateDocument struct {
	Total [6]float64 `json:"total"`
}

// HashrateResponse is the RFC name for HashrateDocument.
type HashrateResponse = HashrateDocument

// MinersCountDocument{Now: 142, Max: 200}
type MinersCountDocument struct {
	Now uint64 `json:"now"`
	Max uint64 `json:"max"`
}

// MinersCountResponse is the RFC name for MinersCountDocument.
type MinersCountResponse = MinersCountDocument

// UpstreamDocument{Active: 1, Sleep: 0, Error: 0, Total: 1, Ratio: 142.0}
type UpstreamDocument struct {
	Active uint64  `json:"active"`
	Sleep  uint64  `json:"sleep"`
	Error  uint64  `json:"error"`
	Total  uint64  `json:"total"`
	Ratio  float64 `json:"ratio"`
}

// UpstreamResponse is the RFC name for UpstreamDocument.
type UpstreamResponse = UpstreamDocument

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

// ResultsResponse is the RFC name for ResultsDocument.
type ResultsResponse = ResultsDocument

// doc := p.WorkersDocument()
// _ = doc.Workers[0][0]
type WorkersDocument struct {
	Mode    string      `json:"mode"`
	Workers []WorkerRow `json:"workers"`
}

// WorkersResponse is the RFC-style name for WorkersDocument.
type WorkersResponse = WorkersDocument

// doc := p.MinersDocument()
// _ = doc.Miners[0][7]
type MinersDocument struct {
	Format []string   `json:"format"`
	Miners []MinerRow `json:"miners"`
}

// MinersResponse is the RFC-style name for MinersDocument.
type MinersResponse = MinersDocument
