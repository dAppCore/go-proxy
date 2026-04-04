package proxy

import (
	"net"
	"net/http"
	"strconv"
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

func TestProxy_allowHTTP_MethodRestricted_Bad(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{},
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

func TestProxy_startHTTP_Good(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Enabled: true,
				Host:    "127.0.0.1",
				Port:    0,
			},
		},
		done: make(chan struct{}),
	}

	if ok := p.startHTTP(); !ok {
		t.Fatal("expected HTTP server to start on a free port")
	}
	p.Stop()
}

func TestProxy_startHTTP_Bad(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral port: %v", err)
	}
	defer listener.Close()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}

	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Enabled: true,
				Host:    host,
				Port:    uint16(portNum),
			},
		},
		done: make(chan struct{}),
	}

	if ok := p.startHTTP(); ok {
		t.Fatal("expected HTTP server start to fail when the port is already in use")
	}
}
