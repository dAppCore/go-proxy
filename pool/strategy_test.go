package pool

import "testing"

func TestPoolStrategy_CurrentIndex_Good(t *testing.T) {
	strategy := &FailoverStrategy{current: 2}
	if got := strategy.CurrentIndex(); got != 2 {
		t.Fatalf("expected current index 2, got %d", got)
	}
}

func TestPoolStrategy_CurrentIndex_Bad(t *testing.T) {
	var strategy *FailoverStrategy
	if got := strategy.CurrentIndex(); got != -1 {
		t.Fatalf("expected nil strategy to return -1, got %d", got)
	}
}

func TestPoolStrategy_Client_Ugly(t *testing.T) {
	client := &StratumClient{}
	strategy := &FailoverStrategy{client: client}
	if got := strategy.Client(); got != client {
		t.Fatalf("expected current client to be returned, got %+v", got)
	}
}
