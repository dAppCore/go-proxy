package proxy

import "testing"

type proxySplitterStub struct {
	stats UpstreamStats
}

func (s *proxySplitterStub) Connect()                    {}
func (s *proxySplitterStub) OnLogin(*LoginEvent)         {}
func (s *proxySplitterStub) OnSubmit(*SubmitEvent)        {}
func (s *proxySplitterStub) OnClose(*CloseEvent)         {}
func (s *proxySplitterStub) Tick(uint64)                 {}
func (s *proxySplitterStub) GC()                         {}
func (s *proxySplitterStub) Upstreams() UpstreamStats    { return s.stats }

func TestProxy_WorkerRecords_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 7, ip: "10.0.0.1", user: "WALLET"}
	workers.OnLogin(Event{Miner: miner})

	p := &Proxy{workers: workers}
	records := p.WorkerRecords()
	if len(records) != 1 {
		t.Fatalf("expected one worker record, got %d", len(records))
	}
	if got := records[0].Name; got != "WALLET" {
		t.Fatalf("expected worker name to be preserved, got %q", got)
	}
	if got := records[0].LastIP; got != "10.0.0.1" {
		t.Fatalf("expected worker IP to be recorded, got %q", got)
	}
}

func TestProxy_WorkerRecords_Bad(t *testing.T) {
	var p *Proxy
	if got := p.WorkerRecords(); got != nil {
		t.Fatalf("expected nil proxy to return nil records, got %v", got)
	}
}

func TestProxy_WorkerRecords_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.WorkerRecords(); got != nil {
		t.Fatalf("expected proxy without workers to return nil records, got %v", got)
	}
}

func TestProxy_MinerCount_Good(t *testing.T) {
	stats := NewStats()
	stats.OnLogin(Event{Miner: &Miner{}})
	p := &Proxy{stats: stats}

	now, max := p.MinerCount()
	if now != 1 || max != 1 {
		t.Fatalf("expected miner count 1/1, got %d/%d", now, max)
	}
}

func TestProxy_MinerCount_Bad(t *testing.T) {
	var p *Proxy
	now, max := p.MinerCount()
	if now != 0 || max != 0 {
		t.Fatalf("expected nil proxy to report zero counts, got %d/%d", now, max)
	}
}

func TestProxy_MinerCount_Ugly(t *testing.T) {
	p := &Proxy{}
	now, max := p.MinerCount()
	if now != 0 || max != 0 {
		t.Fatalf("expected proxy without stats to report zero counts, got %d/%d", now, max)
	}
}

func TestProxy_Upstreams_Good(t *testing.T) {
	p := &Proxy{splitter: &proxySplitterStub{stats: UpstreamStats{Active: 1, Sleep: 2, Error: 3, Total: 6}}}
	if got := p.Upstreams(); got != (UpstreamStats{Active: 1, Sleep: 2, Error: 3, Total: 6}) {
		t.Fatalf("expected upstream counts to be forwarded, got %+v", got)
	}
}

func TestProxy_Upstreams_Bad(t *testing.T) {
	var p *Proxy
	if got := p.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil proxy to return zero upstream stats, got %+v", got)
	}
}

func TestProxy_Upstreams_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected proxy without splitter to return zero upstream stats, got %+v", got)
	}
}

func TestProxy_Events_Good(t *testing.T) {
	bus := NewEventBus()
	p := &Proxy{events: bus}
	if got := p.Events(); got != bus {
		t.Fatalf("expected event bus to be returned, got %+v", got)
	}
}

func TestProxy_Events_Bad(t *testing.T) {
	var p *Proxy
	if got := p.Events(); got != nil {
		t.Fatalf("expected nil proxy to return nil event bus, got %+v", got)
	}
}

func TestProxy_Events_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.Events(); got != nil {
		t.Fatalf("expected proxy without events to return nil event bus, got %+v", got)
	}
}
