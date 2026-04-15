package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAccessLogImpl_Close_Good(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")

	sink := newAccessLogSink(path)
	sink.writeConnectLine("10.0.0.1", "WALLET", "XMRig")
	sink.Close()
	sink.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read access log: %v", err)
	}
	if strings.Count(string(data), "CONNECT") != 1 {
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
