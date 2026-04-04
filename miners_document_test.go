package proxy

import "testing"

func TestProxy_MinersDocument_Good(t *testing.T) {
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

func TestProxy_MinersDocument_Bad(t *testing.T) {
	var p *Proxy

	document := p.MinersDocument()
	if len(document.Miners) != 0 {
		t.Fatalf("expected no miners for a nil proxy, got %d", len(document.Miners))
	}
	if len(document.Format) != 10 {
		t.Fatalf("expected miner format columns to remain stable, got %d", len(document.Format))
	}
}

func TestProxy_MinersDocument_Ugly(t *testing.T) {
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
