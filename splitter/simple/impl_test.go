package simple

import (
	"testing"

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
