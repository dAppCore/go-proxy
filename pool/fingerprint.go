package pool

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"strings"
)

func makeFingerprintVerifier(fingerprint string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	expected, expectedErr := decodeFingerprint(fingerprint)
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if expectedErr != nil {
			return expectedErr
		}
		if len(rawCerts) == 0 {
			return errors.New("missing certificate")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		return verifyCertificateFingerprint(cert, expected)
	}
}

func makeFingerprintConnectionVerifier(fingerprint string) func(tls.ConnectionState) error {
	expected, expectedErr := decodeFingerprint(fingerprint)
	return func(state tls.ConnectionState) error {
		if expectedErr != nil {
			return expectedErr
		}
		if len(state.PeerCertificates) == 0 {
			return errors.New("missing certificate")
		}
		return verifyCertificateFingerprint(state.PeerCertificates[0], expected)
	}
}

func verifyCertificateFingerprint(cert *x509.Certificate, expected []byte) error {
	if cert == nil {
		return errors.New("missing certificate")
	}

	spkiSum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	if subtle.ConstantTimeCompare(spkiSum[:], expected) == 1 {
		return nil
	}

	// Existing configs/tests used SHA-256 over the certificate DER.
	certSum := sha256.Sum256(cert.Raw)
	if subtle.ConstantTimeCompare(certSum[:], expected) == 1 {
		return nil
	}
	return errors.New("tls fingerprint mismatch")
}

func decodeFingerprint(fingerprint string) ([]byte, error) {
	normalized := lowerString(trimString(fingerprint))
	normalized = strings.NewReplacer(":", "", " ", "", "-", "").Replace(normalized)
	expected, err := hex.DecodeString(normalized)
	if err != nil || len(expected) != sha256.Size {
		return nil, errors.New("invalid tls fingerprint")
	}
	return expected, nil
}
