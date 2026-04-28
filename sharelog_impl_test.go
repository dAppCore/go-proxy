package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShareLogImpl_OnAccept_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shares.log")

	sink := newShareLogSink(path)
	sink.OnAccept(Event{Miner: &Miner{user: "WALLET"}, Diff: 1234, Latency: 56})
	sink.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read share log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "ACCEPT  WALLET  diff=1234  latency=56ms") {
		t.Fatalf("expected accept line, got %q", text)
	}
}

func TestShareLogImpl_OnReject_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.OnReject(Event{})
	sink.OnReject(Event{Miner: nil})
}

func TestShareLogImpl_sanitizeLogField_Ugly(t *testing.T) {
	got := sanitizeLogField("bad\"\nreason\r\u2028\u2029\\")
	if !strings.Contains(got, "\\\"") {
		t.Fatalf("expected quotes to be escaped, got %q", got)
	}
	if !strings.Contains(got, "\\\\") {
		t.Fatalf("expected backslashes to be escaped, got %q", got)
	}
	if strings.ContainsAny(got, "\n\r") {
		t.Fatalf("expected newlines to be normalised, got %q", got)
	}
	if strings.Contains(got, "\u2028") || strings.Contains(got, "\u2029") {
		t.Fatalf("expected unicode line separators to be normalised, got %q", got)
	}
}

func TestSharelogImpl_LogSink_SetPath_Good(t *testing.T) {
	sink := newShareLogSink("")
	path := filepath.Join(t.TempDir(), "shares.log")
	sink.SetPath(path)
	if sink.path != path {
		t.Fatalf("expected sink path %q, got %q", path, sink.path)
	}
}

func TestSharelogImpl_LogSink_SetPath_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.SetPath("ignored")
	if sink != nil {
		t.Fatal("expected nil sink to remain nil")
	}
}

func TestSharelogImpl_LogSink_SetPath_Ugly(t *testing.T) {
	first := filepath.Join(t.TempDir(), "first.log")
	second := filepath.Join(t.TempDir(), "second.log")
	sink := newShareLogSink(first)
	sink.OnAccept(Event{Miner: &Miner{user: "wallet"}, Diff: 1})
	sink.SetPath(second)
	if sink.file != nil || sink.path != second {
		t.Fatalf("expected SetPath to close existing file and update path, sink=%+v", sink)
	}
}

func TestSharelogImpl_LogSink_Close_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sink := newShareLogSink(path)
	sink.OnAccept(Event{Miner: &Miner{user: "wallet"}, Diff: 1})
	sink.Close()
	if sink.file != nil {
		t.Fatalf("expected close to clear file handle, sink=%+v", sink)
	}
}

func TestSharelogImpl_LogSink_Close_Bad(t *testing.T) {
	var sink *shareLogSink
	sink.Close()
	if sink != nil {
		t.Fatal("expected nil sink to remain nil")
	}
}

func TestSharelogImpl_LogSink_Close_Ugly(t *testing.T) {
	sink := newShareLogSink(filepath.Join(t.TempDir(), "shares.log"))
	sink.Close()
	sink.Close()
	if sink.file != nil {
		t.Fatalf("expected repeated close to keep file nil, sink=%+v", sink)
	}
}

func TestSharelogImpl_LogSink_OnAccept_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sink := newShareLogSink(path)
	sink.OnAccept(Event{Miner: &Miner{user: "wallet"}, Diff: 12, Latency: 3})
	sink.Close()
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "ACCEPT  wallet  diff=12  latency=3ms") {
		t.Fatalf("expected accept line, data=%q err=%v", string(data), err)
	}
}

func TestSharelogImpl_LogSink_OnAccept_Bad(t *testing.T) {
	sink := newShareLogSink(filepath.Join(t.TempDir(), "shares.log"))
	sink.OnAccept(Event{})
	if sink.file != nil {
		t.Fatalf("expected nil miner accept ignored, sink=%+v", sink)
	}
}

func TestSharelogImpl_LogSink_OnAccept_Ugly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sink := newShareLogSink(path)
	sink.OnAccept(Event{Miner: &Miner{user: "wallet with spaces"}, Diff: ^uint64(0), Latency: 0})
	sink.Close()
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "wallet_with_spaces") {
		t.Fatalf("expected sanitized accept line, data=%q err=%v", string(data), err)
	}
}

func TestSharelogImpl_LogSink_OnReject_Good(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sink := newShareLogSink(path)
	sink.OnReject(Event{Miner: &Miner{user: "wallet"}, Error: "invalid nonce"})
	sink.Close()
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "REJECT  wallet") {
		t.Fatalf("expected reject line, data=%q err=%v", string(data), err)
	}
}

func TestSharelogImpl_LogSink_OnReject_Bad(t *testing.T) {
	sink := newShareLogSink(filepath.Join(t.TempDir(), "shares.log"))
	sink.OnReject(Event{})
	if sink.file != nil {
		t.Fatalf("expected nil miner reject ignored, sink=%+v", sink)
	}
}

func TestSharelogImpl_LogSink_OnReject_Ugly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shares.log")
	sink := newShareLogSink(path)
	sink.OnReject(Event{Miner: &Miner{user: "wallet"}, Error: "bad\"\nreason"})
	sink.Close()
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "bad\\\" reason") {
		t.Fatalf("expected sanitized reject reason, data=%q err=%v", string(data), err)
	}
}
