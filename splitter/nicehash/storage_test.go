package nicehash

import (
	"testing"

	"dappco.re/go/proxy"
)

func TestNonceStorage_AddAndRemove(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)

	if !storage.Add(miner) {
		t.Fatalf("expected add to succeed")
	}
	if miner.FixedByte() != 0 {
		t.Fatalf("expected first slot to be 0, got %d", miner.FixedByte())
	}

	storage.Remove(miner)
	free, dead, active := storage.SlotCount()
	if free != 255 || dead != 1 || active != 0 {
		t.Fatalf("unexpected slot counts: free=%d dead=%d active=%d", free, dead, active)
	}
}
