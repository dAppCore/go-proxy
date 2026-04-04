package proxy

import (
	"crypto/tls"
	"testing"
)

func TestTLS_applyTLSCiphers_Good(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSCiphers(cfg, "ECDHE-RSA-AES128-GCM-SHA256, TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")

	if len(cfg.CipherSuites) != 2 {
		t.Fatalf("expected two recognised cipher suites, got %d", len(cfg.CipherSuites))
	}
}

func TestTLS_applyTLSCiphers_Bad(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSCiphers(cfg, "made-up-cipher-one:made-up-cipher-two")

	if len(cfg.CipherSuites) != 0 {
		t.Fatalf("expected unknown cipher names to be ignored, got %#v", cfg.CipherSuites)
	}
}

func TestTLS_applyTLSCiphers_Ugly(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSCiphers(cfg, "  aes128-sha | ECDHE-RSA-AES256-GCM-SHA384 ; tls_ecdhe_ecdsa_with_aes_256_gcm_sha384  ")

	if len(cfg.CipherSuites) != 3 {
		t.Fatalf("expected mixed separators and casing to be accepted, got %d", len(cfg.CipherSuites))
	}
}
