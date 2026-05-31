package pool

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"testing"
)

// TestVerifyCertificateFingerprint_DERFallback_Good verifies the legacy
// SHA-256-over-DER fingerprint form is accepted when the SPKI form does not
// match — existing pool configs pin the certificate DER digest.
func TestVerifyCertificateFingerprint_DERFallback_Good(t *testing.T) {
	cert, derFingerprint := mustGenerateSelfSignedCert(t)
	expected, err := hex.DecodeString(derFingerprint)
	if err != nil {
		t.Fatalf("decode der fingerprint: %v", err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	if r := verifyCertificateFingerprint(parsed, expected); !r.OK {
		t.Fatalf("expected DER fingerprint to verify, got %v", r.Value)
	}
}

// TestVerifyCertificateFingerprint_NilCert_Bad verifies a nil certificate is
// rejected rather than dereferenced.
func TestVerifyCertificateFingerprint_NilCert_Bad(t *testing.T) {
	if r := verifyCertificateFingerprint(nil, make([]byte, sha256.Size)); r.OK {
		t.Fatal("expected nil certificate to fail verification")
	}
}

// TestVerifyCertificateFingerprint_Mismatch_Ugly verifies that a syntactically
// valid but non-matching expected digest fails closed.
func TestVerifyCertificateFingerprint_Mismatch_Ugly(t *testing.T) {
	cert, _ := mustGenerateSelfSignedCert(t)
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	if r := verifyCertificateFingerprint(parsed, make([]byte, sha256.Size)); r.OK {
		t.Fatal("expected zero-digest mismatch to fail verification")
	}
}

// TestMakeFingerprintVerifier_MalformedCert_Bad verifies an unparseable raw
// certificate is surfaced as an error rather than panicking.
func TestMakeFingerprintVerifier_MalformedCert_Bad(t *testing.T) {
	verifier := makeFingerprintVerifier(hex.EncodeToString(make([]byte, sha256.Size)))
	if err := verifier([][]byte{[]byte("not-a-cert")}, nil); err == nil {
		t.Fatal("expected malformed certificate DER to fail verification")
	}
}

// TestMakeFingerprintVerifier_InvalidFingerprint_Bad verifies a verifier built
// from an unparseable fingerprint string rejects every connection.
func TestMakeFingerprintVerifier_InvalidFingerprint_Bad(t *testing.T) {
	cert, _ := mustGenerateSelfSignedCert(t)
	verifier := makeFingerprintVerifier("zz-not-hex")
	if err := verifier([][]byte{cert.Certificate[0]}, nil); err == nil {
		t.Fatal("expected invalid fingerprint to reject the connection")
	}
}

// TestMakeFingerprintConnectionVerifier_Good verifies the ConnectionState
// variant accepts a peer whose SPKI digest matches the pinned fingerprint.
func TestMakeFingerprintConnectionVerifier_Good(t *testing.T) {
	cert, _ := mustGenerateSelfSignedCert(t)
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	sum := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
	verifier := makeFingerprintConnectionVerifier(hex.EncodeToString(sum[:]))

	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{parsed}}
	if err := verifier(state); err != nil {
		t.Fatalf("expected matching SPKI connection to verify, got %v", err)
	}
}

// TestMakeFingerprintConnectionVerifier_Bad verifies the ConnectionState
// variant rejects an invalid fingerprint, a connection with no peer
// certificates, and a peer whose digest does not match.
func TestMakeFingerprintConnectionVerifier_Bad(t *testing.T) {
	cert, _ := mustGenerateSelfSignedCert(t)
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	if err := makeFingerprintConnectionVerifier("zz-not-hex")(tls.ConnectionState{}); err == nil {
		t.Fatal("expected invalid fingerprint to reject the connection")
	}

	valid := makeFingerprintConnectionVerifier(hex.EncodeToString(make([]byte, sha256.Size)))
	if err := valid(tls.ConnectionState{}); err == nil {
		t.Fatal("expected missing peer certificate to fail")
	}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{parsed}}
	if err := valid(state); err == nil {
		t.Fatal("expected mismatched fingerprint to fail")
	}
}

// TestDecodeFingerprint_Normalisation_Good verifies colon-, space-, and
// dash-separated fingerprints all decode to the same digest.
func TestDecodeFingerprint_Normalisation_Good(t *testing.T) {
	raw := hex.EncodeToString(make([]byte, sha256.Size))
	// Insert separators every two characters to mimic config formats.
	colon := ""
	for i := 0; i < len(raw); i += 2 {
		if colon != "" {
			colon += ":"
		}
		colon += raw[i : i+2]
	}

	cases := []string{raw, colon, "  " + raw + "  ", raw[:8] + "-" + raw[8:]}
	for _, in := range cases {
		r := decodeFingerprint(in)
		if !r.OK {
			t.Fatalf("expected %q to decode, got %v", in, r.Value)
		}
		if got := r.Value.([]byte); len(got) != sha256.Size {
			t.Fatalf("expected %d-byte digest for %q, got %d", sha256.Size, in, len(got))
		}
	}
}

// TestDecodeFingerprint_Bad verifies non-hex input and wrong-length input are
// rejected.
func TestDecodeFingerprint_Bad(t *testing.T) {
	if r := decodeFingerprint("nothex"); r.OK {
		t.Fatal("expected non-hex fingerprint to fail")
	}
	if r := decodeFingerprint("abcd"); r.OK {
		t.Fatal("expected short fingerprint to fail")
	}
}
