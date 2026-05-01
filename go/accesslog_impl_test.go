package proxy

import (
	"testing"
)

func TestAccessLogImpl_Close_Good(t *testing.T) {
	dir := t.TempDir()
	path := pathJoin(dir, "access.log")

	sink := newAccessLogSink(path)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.Close()
	sink.Close()

	data, err := readFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	if countString(string(data), "CONNECT") != 1 {
		t.Fatalf("expected a single connect line, got %q", string(data))
	}
}

func TestAccessLogImpl_Close_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.Close()
	sink.OnLogin(Event{})
	sink.OnClose(Event{})
}

func TestAccessLogImpl_sanitizeLogColumnField_Ugly(t *testing.T) {
	got := sanitizeLogColumnField("A B\tC\nD\rE\u2028F\u2029G\x07H\"\\I")
	want := "A_B_C_D_E_F_G_H\\\"\\\\I"
	if got != want {
		t.Fatalf("expected sanitized column field %q, got %q", want, got)
	}
}

func TestAccesslogImpl_LogSink_SetPath_Good(t *testing.T) {
	sink := newAccessLogSink("")
	path := pathJoin(t.TempDir(), "access.log")
	sink.SetPath(path)
	if sink.path != path {
		t.Fatalf("expected sink path %q, got %q", path, sink.path)
	}
}

func TestAccesslogImpl_LogSink_SetPath_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.SetPath("ignored")
	if sink != nil {
		t.Fatal("expected nil sink to remain nil")
	}
}

func TestAccesslogImpl_LogSink_SetPath_Ugly(t *testing.T) {
	first := pathJoin(t.TempDir(), "first.log")
	second := pathJoin(t.TempDir(), "second.log")
	sink := newAccessLogSink(first)
	sink.writeConnectLine("10.0.0.1", "wallet", "agent")
	sink.SetPath(second)
	if sink.file != nil || sink.path != second {
		t.Fatalf("expected SetPath to close existing file and update path, sink=%+v", sink)
	}
}

func TestAccesslogImpl_LogSink_Close_Good(t *testing.T) {
	path := pathJoin(t.TempDir(), "access.log")
	sink := newAccessLogSink(path)
	sink.writeConnectLine("10.0.0.1", "wallet", "agent")
	sink.Close()
	if sink.file != nil {
		t.Fatalf("expected close to clear file handle, sink=%+v", sink)
	}
}

func TestAccesslogImpl_LogSink_Close_Bad(t *testing.T) {
	var sink *accessLogSink
	sink.Close()
	if sink != nil {
		t.Fatal("expected nil sink to remain nil")
	}
}

func TestAccesslogImpl_LogSink_Close_Ugly(t *testing.T) {
	sink := newAccessLogSink(pathJoin(t.TempDir(), "access.log"))
	sink.Close()
	sink.Close()
	if sink.file != nil {
		t.Fatalf("expected repeated close to keep file nil, sink=%+v", sink)
	}
}

func TestAccesslogImpl_LogSink_OnLogin_Good(t *testing.T) {
	path := pathJoin(t.TempDir(), "access.log")
	sink := newAccessLogSink(path)
	sink.OnLogin(Event{Miner: &Miner{ip: "10.0.0.1", user: "wallet", agent: "agent"}})
	sink.Close()
	data, err := readFile(path)
	if err != nil || !containsString(string(data), "CONNECT") {
		t.Fatalf("expected connect line, data=%q err=%v", string(data), err)
	}
}

func TestAccesslogImpl_LogSink_OnLogin_Bad(t *testing.T) {
	sink := newAccessLogSink(pathJoin(t.TempDir(), "access.log"))
	sink.OnLogin(Event{})
	if sink.file != nil {
		t.Fatalf("expected nil miner login ignored, sink=%+v", sink)
	}
}

func TestAccesslogImpl_LogSink_OnLogin_Ugly(t *testing.T) {
	path := pathJoin(t.TempDir(), "access.log")
	sink := newAccessLogSink(path)
	sink.OnLogin(Event{Miner: &Miner{user: "wallet with spaces", agent: "agent\nname"}})
	sink.Close()
	data, err := readFile(path)
	if err != nil || !containsString(string(data), "wallet_with_spaces") {
		t.Fatalf("expected sanitized login line, data=%q err=%v", string(data), err)
	}
}

func TestAccesslogImpl_LogSink_OnClose_Good(t *testing.T) {
	path := pathJoin(t.TempDir(), "access.log")
	sink := newAccessLogSink(path)
	sink.OnClose(Event{Miner: &Miner{ip: "10.0.0.1", user: "wallet", rx: 1, tx: 2}})
	sink.Close()
	data, err := readFile(path)
	if err != nil || !containsString(string(data), "CLOSE") {
		t.Fatalf("expected close line, data=%q err=%v", string(data), err)
	}
}

func TestAccesslogImpl_LogSink_OnClose_Bad(t *testing.T) {
	sink := newAccessLogSink(pathJoin(t.TempDir(), "access.log"))
	sink.OnClose(Event{})
	if sink.file != nil {
		t.Fatalf("expected nil miner close ignored, sink=%+v", sink)
	}
}

func TestAccesslogImpl_LogSink_OnClose_Ugly(t *testing.T) {
	path := pathJoin(t.TempDir(), "access.log")
	sink := newAccessLogSink(path)
	sink.OnClose(Event{Miner: &Miner{user: "wallet", rx: ^uint64(0), tx: ^uint64(0)}})
	sink.Close()
	data, err := readFile(path)
	if err != nil || !containsString(string(data), "rx=18446744073709551615") {
		t.Fatalf("expected max counters in close line, data=%q err=%v", string(data), err)
	}
}
