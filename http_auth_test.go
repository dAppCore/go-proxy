package proxy

import (
	"net/http"
	"testing"
)

func TestProxy_allowHTTP_Good(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Restricted:  true,
				AccessToken: "secret",
			},
		},
	}

	status, ok := p.allowHTTP(&http.Request{
		Method: http.MethodGet,
		Header: http.Header{
			"Authorization": []string{"Bearer secret"},
		},
	})
	if !ok {
		t.Fatalf("expected authorised request to pass, got status %d", status)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, status)
	}
}

func TestProxy_allowHTTP_Bad(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Restricted: true,
			},
		},
	}

	status, ok := p.allowHTTP(&http.Request{Method: http.MethodPost})
	if ok {
		t.Fatal("expected non-GET request to be rejected")
	}
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestProxy_allowHTTP_Ugly(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				AccessToken: "secret",
			},
		},
	}

	status, ok := p.allowHTTP(&http.Request{
		Method: http.MethodGet,
		Header: http.Header{
			"Authorization": []string{"Bearer wrong"},
		},
	})
	if ok {
		t.Fatal("expected invalid token to be rejected")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, status)
	}
}
