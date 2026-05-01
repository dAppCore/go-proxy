package pool

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"

	core "dappco.re/go"
)

type peerCertificateVerifier func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
type connectionVerifier func(tls.ConnectionState) error

func makeFingerprintVerifier(fingerprint string) peerCertificateVerifier {
	expected := decodeFingerprint(fingerprint)
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if !expected.OK {
			return expected.Value.(error)
		}
		if len(rawCerts) == 0 {
			return core.NewError("missing certificate")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		r := verifyCertificateFingerprint(cert, expected.Value.([]byte))
		if !r.OK {
			return r.Value.(error)
		}
		return nil
	}
}

func makeFingerprintConnectionVerifier(fingerprint string) connectionVerifier {
	expected := decodeFingerprint(fingerprint)
	return func(state tls.ConnectionState) error {
		if !expected.OK {
			return expected.Value.(error)
		}
		if len(state.PeerCertificates) == 0 {
			return core.NewError("missing certificate")
		}
		r := verifyCertificateFingerprint(state.PeerCertificates[0], expected.Value.([]byte))
		if !r.OK {
			return r.Value.(error)
		}
		return nil
	}
}

func verifyCertificateFingerprint(cert *x509.Certificate, expected []byte) core.Result {
	if cert == nil {
		return core.Fail(core.NewError("missing certificate"))
	}

	spkiSum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	if subtle.ConstantTimeCompare(spkiSum[:], expected) == 1 {
		return core.Ok(nil)
	}

	// Existing configs/tests used SHA-256 over the certificate DER.
	certSum := sha256.Sum256(cert.Raw)
	if subtle.ConstantTimeCompare(certSum[:], expected) == 1 {
		return core.Ok(nil)
	}
	return core.Fail(core.NewError("tls fingerprint mismatch"))
}

func decodeFingerprint(fingerprint string) core.Result {
	normalized := lowerString(trimString(fingerprint))
	normalized = core.Replace(normalized, ":", "")
	normalized = core.Replace(normalized, " ", "")
	normalized = core.Replace(normalized, "-", "")
	expected, err := hex.DecodeString(normalized)
	if err != nil || len(expected) != sha256.Size {
		return core.Fail(core.NewError("invalid tls fingerprint"))
	}
	return core.Ok(expected)
}
