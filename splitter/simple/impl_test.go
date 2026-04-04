package simple

import (
	"testing"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type activeStrategy struct{}

func (a activeStrategy) Connect()                                    {}
func (a activeStrategy) Submit(string, string, string, string) int64 { return 0 }
func (a activeStrategy) Disconnect()                                 {}
func (a activeStrategy) IsActive() bool                              { return true }

func TestSimpleSplitter_OnLogin_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	miner := &proxy.Miner{}
	job := proxy.Job{JobID: "job-1", Blob: "blob"}
	mapper := &SimpleMapper{
		id:         7,
		strategy:   activeStrategy{},
		currentJob: job,
		idleAt:     time.Now(),
	}
	splitter.idle[mapper.id] = mapper

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if miner.RouteID() != mapper.id {
		t.Fatalf("expected reclaimed mapper route id %d, got %d", mapper.id, miner.RouteID())
	}
	if got := miner.CurrentJob().JobID; got != job.JobID {
		t.Fatalf("expected current job to be restored on reuse, got %q", got)
	}
}

func TestSimpleSplitter_OnLogin_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	miner := &proxy.Miner{}
	expired := &SimpleMapper{
		id:       7,
		strategy: activeStrategy{},
		idleAt:   time.Now().Add(-time.Minute),
	}
	splitter.idle[expired.id] = expired

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if miner.RouteID() == expired.id {
		t.Fatalf("expected expired mapper not to be reclaimed")
	}
	if miner.RouteID() != 0 {
		t.Fatalf("expected a new mapper to be allocated, got route id %d", miner.RouteID())
	}
	if len(splitter.active) != 1 {
		t.Fatalf("expected one active mapper, got %d", len(splitter.active))
	}
	if len(splitter.idle) != 1 {
		t.Fatalf("expected expired mapper to remain idle until GC, got %d idle mappers", len(splitter.idle))
	}
}

func TestSimpleSplitter_Upstreams_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: activeStrategy{}}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: activeStrategy{}, idleAt: time.Now()}

	stats := splitter.Upstreams()

	if stats.Active != 1 {
		t.Fatalf("expected one active upstream, got %d", stats.Active)
	}
	if stats.Sleep != 1 {
		t.Fatalf("expected one sleeping upstream, got %d", stats.Sleep)
	}
	if stats.Error != 0 {
		t.Fatalf("expected no error upstreams, got %d", stats.Error)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total upstreams to be 2, got %d", stats.Total)
	}
}

func TestSimpleSplitter_Upstreams_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: activeStrategy{}, stopped: true}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: activeStrategy{}, stopped: true, idleAt: time.Now()}

	stats := splitter.Upstreams()

	if stats.Active != 0 {
		t.Fatalf("expected no active upstreams, got %d", stats.Active)
	}
	if stats.Sleep != 0 {
		t.Fatalf("expected no sleeping upstreams, got %d", stats.Sleep)
	}
	if stats.Error != 2 {
		t.Fatalf("expected both upstreams to be counted as error, got %d", stats.Error)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total upstreams to be 2, got %d", stats.Total)
	}
}
