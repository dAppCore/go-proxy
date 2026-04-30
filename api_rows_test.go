package proxy

import (
	"encoding/json"
	"testing"
)

func TestApiRows_Documents_Good(t *testing.T) {
	if SummaryDocumentVersion != "1.0.0" {
		t.Fatalf("unexpected summary version %q", SummaryDocumentVersion)
	}
	if len(MinersDocumentFormat) != 10 {
		t.Fatalf("expected 10 miner columns, got %d", len(MinersDocumentFormat))
	}

	summary := SummaryDocument{
		Version: SummaryDocumentVersion,
		Mode:    "nicehash",
	}
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary document: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected summary document json to be non-empty")
	}
}

func TestApiRows_Documents_Bad(t *testing.T) {
	if MinersDocumentFormat[0] != "id" || MinersDocumentFormat[len(MinersDocumentFormat)-1] != "agent" {
		t.Fatalf("unexpected miners document format: %v", MinersDocumentFormat)
	}
}

func TestApiRows_Documents_Ugly(t *testing.T) {
	if len(WorkerRow{}) != 13 {
		t.Fatalf("expected worker row width 13, got %d", len(WorkerRow{}))
	}
	if len(MinerRow{}) != 10 {
		t.Fatalf("expected miner row width 10, got %d", len(MinerRow{}))
	}
}
