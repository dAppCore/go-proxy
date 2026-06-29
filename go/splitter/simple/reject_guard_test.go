package simple

import (
	"testing"

	"dappco.re/go/proxy"
)

// TestSimpleMapper_rejectInvalidJobLocked_NilGuards verifies the invalid-job
// reject helper tolerates a nil event and a nil miner without panicking or
// dispatching — a malformed submit must never crash the splitter.
func TestSimpleMapper_rejectInvalidJobLocked_NilGuards(t *testing.T) {
	mapper := NewSimpleMapper(1, &simpleStrategySpy{active: true})

	mapper.rejectInvalidJobLocked(nil, proxy.Job{})
	mapper.rejectInvalidJobLocked(&proxy.SubmitEvent{Miner: nil}, proxy.Job{})
}

// TestSimpleMapper_rejectUnavailableLocked_NilGuards verifies the unavailable
// reject helper tolerates a nil event and a nil miner.
func TestSimpleMapper_rejectUnavailableLocked_NilGuards(t *testing.T) {
	mapper := NewSimpleMapper(1, &simpleStrategySpy{active: true})

	mapper.rejectUnavailableLocked(nil, proxy.Job{})
	mapper.rejectUnavailableLocked(&proxy.SubmitEvent{Miner: nil}, proxy.Job{})
}

// TestSimpleMapper_rejectUnavailableSubmit_NilGuards verifies the package-level
// unavailable reject helper tolerates a nil event and a nil miner even when no
// event bus is wired.
func TestSimpleMapper_rejectUnavailableSubmit_NilGuards(t *testing.T) {
	rejectUnavailableSubmit(nil, nil)
	rejectUnavailableSubmit(nil, &proxy.SubmitEvent{Miner: nil})
}
