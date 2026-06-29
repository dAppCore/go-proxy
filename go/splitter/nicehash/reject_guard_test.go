package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
)

// TestNonceMapper_rejectUnavailableLocked_NilGuards verifies the unavailable
// reject helper tolerates a nil event and a nil miner without panicking — a
// malformed submit must never crash the splitter.
func TestNonceMapper_rejectUnavailableLocked_NilGuards(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{active: true})

	mapper.rejectUnavailableLocked(nil, proxy.Job{})
	mapper.rejectUnavailableLocked(&proxy.SubmitEvent{Miner: nil}, proxy.Job{})
}

// TestNonceMapper_rejectUnavailableSubmit_NilGuards verifies the package-level
// unavailable reject helper tolerates a nil event and a nil miner even when no
// event bus is wired.
func TestNonceMapper_rejectUnavailableSubmit_NilGuards(t *testing.T) {
	rejectUnavailableSubmit(nil, nil)
	rejectUnavailableSubmit(nil, &proxy.SubmitEvent{Miner: nil})
}
