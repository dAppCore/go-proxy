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

import (
	"encoding/json"
	"net/http"
	"time"

	"dappco.re/go/proxy"
)

// Router matches the standard http.ServeMux registration shape.
type Router interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// SummaryResponse is the /1/summary JSON body.
//
//	{"version":"1.0.0","mode":"nicehash","hashrate":{"total":[...]}, ...}
type SummaryResponse struct {
	Version         string                                 `json:"version"`
	Mode            string                                 `json:"mode"`
	Hashrate        HashrateResponse                       `json:"hashrate"`
	Miners          MinersCountResponse                    `json:"miners"`
	Workers         uint64                                 `json:"workers"`
	Upstreams       UpstreamResponse                       `json:"upstreams"`
	Results         ResultsResponse                        `json:"results"`
	CustomDiffStats map[uint64]proxy.CustomDiffBucketStats `json:"custom_diff_stats,omitempty"`
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

// RegisterRoutes wires the monitoring endpoints onto the supplied router.
//
//	proxyapi.RegisterRoutes(mux, p)
//	// GET /1/summary, /1/workers, and /1/miners are now live.
func RegisterRoutes(r Router, p *proxy.Proxy) {
	if r == nil || p == nil {
		return
	}
	r.HandleFunc("/1/summary", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, summaryResponse(p))
	})
	r.HandleFunc("/1/workers", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, workersResponse(p))
	})
	r.HandleFunc("/1/miners", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, minersResponse(p))
	})
}

func summaryResponse(p *proxy.Proxy) SummaryResponse {
	summary := p.Summary()
	now, max := p.MinerCount()
	upstreams := p.Upstreams()
	return SummaryResponse{
		Version: "1.0.0",
		Mode:    p.Mode(),
		Hashrate: HashrateResponse{
			Total: summary.Hashrate,
		},
		CustomDiffStats: summary.CustomDiffStats,
		Miners: MinersCountResponse{
			Now: now,
			Max: max,
		},
		Workers: uint64(len(p.WorkerRecords())),
		Upstreams: UpstreamResponse{
			Active: upstreams.Active,
			Sleep:  upstreams.Sleep,
			Error:  upstreams.Error,
			Total:  upstreams.Total,
			Ratio:  upstreamRatio(now, upstreams.Total),
		},
		Results: ResultsResponse{
			Accepted:    summary.Accepted,
			Rejected:    summary.Rejected,
			Invalid:     summary.Invalid,
			Expired:     summary.Expired,
			AvgTime:     summary.AvgTime,
			Latency:     summary.AvgLatency,
			HashesTotal: summary.Hashes,
			Best:        summary.TopDiff,
		},
	}
}

func workersResponse(p *proxy.Proxy) any {
	records := p.WorkerRecords()
	rows := make([]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, []any{
			record.Name,
			record.LastIP,
			record.Connections,
			record.Accepted,
			record.Rejected,
			record.Invalid,
			record.Hashes,
			unixOrZero(record.LastHashAt),
			record.Hashrate(60),
			record.Hashrate(600),
			record.Hashrate(3600),
			record.Hashrate(43200),
			record.Hashrate(86400),
		})
	}
	return map[string]any{
		"mode":    string(p.WorkersMode()),
		"workers": rows,
	}
}

func minersResponse(p *proxy.Proxy) any {
	records := p.MinerSnapshots()
	rows := make([]any, 0, len(records))
	for _, miner := range records {
		rows = append(rows, []any{
			miner.ID,
			miner.IP,
			miner.TX,
			miner.RX,
			miner.State,
			miner.Diff,
			miner.User,
			miner.Password,
			miner.RigID,
			miner.Agent,
		})
	}
	return map[string]any{
		"format": []string{"id", "ip", "tx", "rx", "state", "diff", "user", "password", "rig_id", "agent"},
		"miners": rows,
	}
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func upstreamRatio(now, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(now) / float64(total)
}

func unixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}
