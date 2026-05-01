package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
)

type upstreamStateStrategy struct {
	active bool
}

func (s *upstreamStateStrategy) Connect() {}

func (s *upstreamStateStrategy) Submit(jobID, nonce, result, algo string) int64 {
	return 0
}

func (s *upstreamStateStrategy) Disconnect() {}

func (s *upstreamStateStrategy) IsActive() bool { return s.active }

func TestUpstreams_NonceSplitter_Upstreams_Good(t *testing.T) {
	splitter := &NonceSplitter{
		mappers: []*NonceMapper{
			{strategy: &upstreamStateStrategy{active: true}, active: true},
			{strategy: &upstreamStateStrategy{active: false}, active: false, suspended: 1},
		},
	}

	stats := splitter.Upstreams()

	if stats.Active != 1 {
		t.Fatalf("expected one active upstream, got %d", stats.Active)
	}
	if stats.Error != 1 {
		t.Fatalf("expected one error upstream, got %d", stats.Error)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total to equal active + sleep + error, got %d", stats.Total)
	}
}

func TestUpstreams_NonceSplitter_Upstreams_Bad(t *testing.T) {
	var splitter *NonceSplitter

	stats := splitter.Upstreams()

	if stats != (proxy.UpstreamStats{}) {
		t.Fatalf("expected zero-value stats for nil splitter, got %+v", stats)
	}
}

func TestUpstreams_NonceSplitter_Upstreams_Ugly(t *testing.T) {
	splitter := &NonceSplitter{
		mappers: []*NonceMapper{
			{strategy: &upstreamStateStrategy{active: false}, active: false},
		},
	}

	stats := splitter.Upstreams()

	if stats.Error != 1 {
		t.Fatalf("expected an unready mapper to be counted as error, got %+v", stats)
	}
	if stats.Total != 1 {
		t.Fatalf("expected total to remain internally consistent, got %+v", stats)
	}
}
