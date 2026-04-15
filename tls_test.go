package proxy

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestTLS_parseTLSVersion_Good(t *testing.T) {
	cases := map[string]uint16{
		"tls1":   tls.VersionTLS10,
		"1.1":    tls.VersionTLS11,
		"tls1.2": tls.VersionTLS12,
		"1.3":    tls.VersionTLS13,
	}
	for input, expected := range cases {
		if got := parseTLSVersion(input); got != expected {
			t.Fatalf("expected %q to map to %d, got %d", input, expected, got)
		}
	}
}

func TestTLS_parseTLSVersion_Bad(t *testing.T) {
	if got := parseTLSVersion("tls2.0"); got != 0 {
		t.Fatalf("expected unknown version to return 0, got %d", got)
	}
}

func TestTLS_parseTLSVersion_Ugly(t *testing.T) {
	if got := parseTLSVersion("  TLSV1.3  "); got != tls.VersionTLS13 {
		t.Fatalf("expected whitespace and casing to be ignored, got %d", got)
	}
}

func TestTLS_applyTLSProtocols_Good(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSProtocols(cfg, "tls1.1-tls1.3")

	if cfg.MinVersion != tls.VersionTLS11 {
		t.Fatalf("expected min version TLS1.1, got %d", cfg.MinVersion)
	}
	if cfg.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("expected max version TLS1.3, got %d", cfg.MaxVersion)
	}
}

func TestTLS_applyTLSProtocols_Bad(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSProtocols(cfg, "made-up-version")

	if cfg.MinVersion != 0 || cfg.MaxVersion != 0 {
		t.Fatalf("expected invalid protocol strings to be ignored, got min=%d max=%d", cfg.MinVersion, cfg.MaxVersion)
	}
}

func TestTLS_applyTLSProtocols_Ugly(t *testing.T) {
	cfg := &tls.Config{}

	applyTLSProtocols(cfg, " tls1 ; tls1.2-tls1.3 | tls1.0 ")

	if cfg.MinVersion != tls.VersionTLS10 {
		t.Fatalf("expected lowest parsed version to be TLS1.0, got %d", cfg.MinVersion)
	}
	if cfg.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("expected highest parsed version to be TLS1.3, got %d", cfg.MaxVersion)
	}
}

func TestTLS_buildTLSConfig_Good(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertPair(t, dir)

	cfg, result := buildTLSConfig(TLSConfig{
		Enabled:   true,
		CertFile:  certFile,
		KeyFile:   keyFile,
		Protocols: "tls1.2-tls1.3",
		Ciphers:   "ECDHE-RSA-AES128-GCM-SHA256",
	})
	if !result.OK {
		t.Fatalf("expected TLS config to load, got error: %v", result.Error)
	}
	if cfg == nil {
		t.Fatal("expected TLS config")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected one certificate, got %d", len(cfg.Certificates))
	}
	if cfg.MinVersion != tls.VersionTLS12 || cfg.MaxVersion != tls.VersionTLS13 {
		t.Fatalf("expected protocol bounds to be applied, got min=%d max=%d", cfg.MinVersion, cfg.MaxVersion)
	}
	if len(cfg.CipherSuites) != 1 {
		t.Fatalf("expected one cipher suite, got %d", len(cfg.CipherSuites))
	}
}

func TestTLS_buildTLSConfig_Bad(t *testing.T) {
	_, result := buildTLSConfig(TLSConfig{
		Enabled:  true,
		CertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
		KeyFile:  filepath.Join(t.TempDir(), "missing-key.pem"),
	})
	if result.OK {
		t.Fatal("expected missing certificate files to fail")
	}
}

func TestTLS_buildTLSConfig_Ugly(t *testing.T) {
	cfg, result := buildTLSConfig(TLSConfig{Enabled: false})
	if !result.OK {
		t.Fatalf("expected disabled TLS config to succeed, got %v", result.Error)
	}
	if cfg != nil {
		t.Fatalf("expected disabled TLS config to return nil config, got %#v", cfg)
	}
}

func TestTLS_sha256Hex_Good(t *testing.T) {
	if got := sha256Hex([]byte("abc")); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("unexpected sha256 hex: %s", got)
	}
}

func TestTLS_sha256Hex_Bad(t *testing.T) {
	if got := sha256Hex(nil); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("unexpected sha256 hex for empty input: %s", got)
	}
}

func TestTLS_sha256Hex_Ugly(t *testing.T) {
	if got := sha256Hex([]byte{0x00, 0xff, 0x10}); len(got) != 64 {
		t.Fatalf("expected 64-char hex digest, got %q", got)
	}
}

func writeTestCertPair(t *testing.T, dir string) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "proxy-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		_ = certOut.Close()
		t.Fatalf("encode cert: %v", err)
	}
	if err := certOut.Close(); err != nil {
		t.Fatalf("close cert file: %v", err)
	}

	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		_ = keyOut.Close()
		t.Fatalf("encode key: %v", err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatalf("close key file: %v", err)
	}

	return certFile, keyFile
}
