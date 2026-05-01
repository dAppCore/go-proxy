package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMinersDocument_Proxy_MinersDocument_Good(t *testing.T) {
	p := &Proxy{
		miners: map[int64]*Miner{
			1: {
				id:       1,
				ip:       "10.0.0.1:49152",
				tx:       4096,
				rx:       512,
				state:    MinerStateReady,
				diff:     100000,
				user:     "WALLET",
				password: "secret",
				rigID:    "rig-alpha",
				agent:    "XMRig/6.21.0",
			},
		},
	}

	document := p.MinersDocument()
	if len(document.Miners) != 1 {
		t.Fatalf("expected one miner row, got %d", len(document.Miners))
	}
	row := document.Miners[0]
	if len(row) != 10 {
		t.Fatalf("expected 10 miner columns, got %d", len(row))
	}
	if row[7] != "********" {
		t.Fatalf("expected masked password, got %#v", row[7])
	}
}

func TestMinersDocument_Proxy_MinersDocument_Bad(t *testing.T) {
	var p *Proxy

	document := p.MinersDocument()
	if len(document.Miners) != 0 {
		t.Fatalf("expected no miners for a nil proxy, got %d", len(document.Miners))
	}
	if len(document.Format) != 10 {
		t.Fatalf("expected miner format columns to remain stable, got %d", len(document.Format))
	}
}

func TestMinersDocument_Proxy_MinersDocument_Ugly(t *testing.T) {
	p := &Proxy{
		miners: map[int64]*Miner{
			1: {
				id:       1,
				ip:       "10.0.0.1:49152",
				tx:       4096,
				rx:       512,
				state:    MinerStateReady,
				diff:     100000,
				user:     "WALLET",
				password: "secret-a",
				rigID:    "rig-alpha",
				agent:    "XMRig/6.21.0",
			},
			2: {
				id:       2,
				ip:       "10.0.0.2:49152",
				tx:       2048,
				rx:       256,
				state:    MinerStateWaitReady,
				diff:     50000,
				user:     "WALLET2",
				password: "secret-b",
				rigID:    "rig-beta",
				agent:    "XMRig/6.22.0",
			},
		},
	}

	document := p.MinersDocument()
	if len(document.Miners) != 2 {
		t.Fatalf("expected two miner rows, got %d", len(document.Miners))
	}
	for i, row := range document.Miners {
		if row[7] != "********" {
			t.Fatalf("expected masked password in row %d, got %#v", i, row[7])
		}
	}
}

type upstreamStubSplitter struct {
	stats UpstreamStats
}

func (s upstreamStubSplitter) Connect()                 {}
func (s upstreamStubSplitter) OnLogin(*LoginEvent)      {}
func (s upstreamStubSplitter) OnSubmit(*SubmitEvent)    {}
func (s upstreamStubSplitter) OnClose(*CloseEvent)      {}
func (s upstreamStubSplitter) Tick(uint64)              {}
func (s upstreamStubSplitter) GC()                      {}
func (s upstreamStubSplitter) Upstreams() UpstreamStats { return s.stats }

func TestMinersDocument_Proxy_SummaryDocument_Good(t *testing.T) {
	stats := NewStats()
	stats.OnLogin(Event{Miner: &Miner{}})
	stats.OnAccept(Event{Diff: 100, Latency: 12})

	workers := NewWorkers(WorkersByRigID, nil)
	miner := &Miner{id: 1, ip: "10.0.0.1:49152", user: "WALLET", rigID: "rig-alpha"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 100})

	p := &Proxy{
		config:  &Config{Mode: "nicehash", Workers: WorkersByRigID},
		stats:   stats,
		workers: workers,
		events:  NewEventBus(),
		splitter: upstreamStubSplitter{stats: UpstreamStats{
			Active: 1,
			Total:  1,
		}},
	}
	p.minersMu.Lock()
	p.miners = map[int64]*Miner{miner.id: miner}
	p.minersMu.Unlock()

	doc := p.SummaryDocument()
	if doc.Version != SummaryDocumentVersion {
		t.Fatalf("expected summary version %q, got %q", SummaryDocumentVersion, doc.Version)
	}
	if doc.Mode != "nicehash" {
		t.Fatalf("expected mode nicehash, got %q", doc.Mode)
	}
	if doc.Miners.Now != 1 || doc.Workers != 1 {
		t.Fatalf("expected one miner and one worker, got miners=%+v workers=%d", doc.Miners, doc.Workers)
	}
	if doc.Upstreams.Ratio != 1 {
		t.Fatalf("expected upstream ratio 1, got %f", doc.Upstreams.Ratio)
	}
	if doc.Results.Accepted != 1 || doc.Results.HashesTotal != 100 {
		t.Fatalf("expected results to reflect stats, got %+v", doc.Results)
	}
	if p.Events() == nil {
		t.Fatal("expected events bus to be available")
	}
}

func TestMinersDocument_Proxy_SummaryDocument_Bad(t *testing.T) {
	var p *Proxy

	doc := p.SummaryDocument()
	if doc.Version != SummaryDocumentVersion || doc.Mode != "" || doc.Workers != 0 || doc.Miners.Now != 0 || doc.Miners.Max != 0 || doc.Results != (ResultsDocument{}) {
		t.Fatalf("expected zero summary for nil proxy, got %+v", doc)
	}
}

func TestMinersDocument_Proxy_SummaryDocument_Ugly(t *testing.T) {
	p := &Proxy{
		config:  &Config{Mode: "simple", Workers: WorkersDisabled},
		stats:   NewStats(),
		workers: NewWorkers(WorkersDisabled, nil),
		splitter: upstreamStubSplitter{stats: UpstreamStats{
			Active: 0,
			Total:  0,
		}},
	}
	doc := p.SummaryDocument()
	if doc.Upstreams.Ratio != 0 {
		t.Fatalf("expected zero ratio with no upstreams, got %f", doc.Upstreams.Ratio)
	}
}

func TestMinersDocument_Proxy_WorkersDocument_Good(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	miner := &Miner{id: 1, ip: "10.0.0.1", user: "WALLET", rigID: "rig-alpha"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 120})

	p := &Proxy{
		config:  &Config{Mode: "simple", Workers: WorkersByRigID},
		workers: workers,
	}

	doc := p.WorkersDocument()
	if doc.Mode != string(WorkersByRigID) {
		t.Fatalf("expected workers mode %q, got %q", WorkersByRigID, doc.Mode)
	}
	if len(doc.Workers) != 1 {
		t.Fatalf("expected one worker row, got %d", len(doc.Workers))
	}
	row := doc.Workers[0]
	if row[0] != "rig-alpha" || row[1] != "10.0.0.1" {
		t.Fatalf("unexpected worker row: %#v", row)
	}
	if row[7] == int64(0) {
		t.Fatalf("expected last hash time to be populated, got %#v", row[7])
	}
}

func TestMinersDocument_Proxy_WorkersDocument_Bad(t *testing.T) {
	var p *Proxy

	doc := p.WorkersDocument()
	if doc.Mode != string(WorkersDisabled) || len(doc.Workers) != 0 {
		t.Fatalf("expected zero workers document for nil proxy, got %+v", doc)
	}
}

func TestMinersDocument_Proxy_WorkersDocument_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	workers.OnLogin(Event{Miner: &Miner{id: 1, ip: "10.0.0.1", user: "WALLET", rigID: "rig-ugly"}})
	p := &Proxy{
		config:  &Config{Mode: "simple", Workers: WorkersByRigID},
		workers: workers,
	}
	doc := p.WorkersDocument()
	if doc.Mode != string(WorkersByRigID) {
		t.Fatalf("expected workers mode, got %q", doc.Mode)
	}
	if len(doc.Workers) != 1 {
		t.Fatalf("expected one worker row, got %d", len(doc.Workers))
	}
	if doc.Workers[0][7] != int64(0) {
		t.Fatalf("expected zero timestamp for inactive worker, got %#v", doc.Workers[0][7])
	}
}

func TestProxy_writeJSONResponse_Good(t *testing.T) {
	recorder := httptest.NewRecorder()
	payload := map[string]any{"ok": true}

	(&Proxy{}).writeJSONResponse(recorder, payload)

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected content type application/json, got %q", got)
	}
	var decoded map[string]any
	if err := testJSONUnmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if decoded["ok"] != true {
		t.Fatalf("expected payload to round-trip, got %#v", decoded)
	}
}

func TestProxy_writeJSONResponse_Bad(t *testing.T) {
	(&Proxy{}).writeJSONResponse(httptest.NewRecorder(), struct {
		Ch chan int `json:"ch"`
	}{})
}

func TestProxy_writeJSONResponse_Ugly(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&Proxy{}).writeJSONResponse(recorder, map[string]any{"at": time.Unix(0, 0).UTC()})
	if recorder.Body.Len() == 0 {
		t.Fatal("expected JSON response body")
	}
	if got := recorder.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("expected default status code 200, got %d", got)
	}
}
