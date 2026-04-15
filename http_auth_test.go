package proxy

import (
	"net"
	"net/http"
	"net/http/httptest"
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

	status, ok := p.AllowMonitoringRequest(&http.Request{
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

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodPost})
	if ok {
		t.Fatal("expected non-GET request to be rejected")
	}
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestProxy_allowHTTP_Unrestricted_Good(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{},
		},
	}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if !ok {
		t.Fatalf("expected unrestricted request to pass, got status %d", status)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, status)
	}
}

func TestProxy_allowHTTP_Unrestricted_Bad(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{},
		},
	}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodPost})
	if ok {
		t.Fatal("expected unrestricted non-GET request to be rejected")
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

	status, ok := p.AllowMonitoringRequest(&http.Request{
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

func TestProxy_allowHTTP_NilConfig_Ugly(t *testing.T) {
	p := &Proxy{}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if ok {
		t.Fatal("expected nil config request to be rejected")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, status)
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

	if ok := p.startMonitoringServer(); !ok {
		t.Fatal("expected HTTP server to start on a free port")
	}
	p.lifecycleMu.RLock()
	httpServer := p.httpServer
	p.lifecycleMu.RUnlock()
	if httpServer == nil {
		t.Fatal("expected HTTP server instance to be recorded")
	}
	if httpServer.ReadHeaderTimeout == 0 {
		t.Fatal("expected monitoring server to set a read-header timeout")
	}
	if httpServer.ReadTimeout == 0 {
		t.Fatal("expected monitoring server to set a read timeout")
	}
	if httpServer.WriteTimeout == 0 {
		t.Fatal("expected monitoring server to set a write timeout")
	}
	if httpServer.IdleTimeout == 0 {
		t.Fatal("expected monitoring server to set an idle timeout")
	}
	p.Stop()
}

func TestProxy_startHTTP_NilConfig_Bad(t *testing.T) {
	p := &Proxy{}

	if ok := p.startMonitoringServer(); ok {
		t.Fatal("expected nil config to skip HTTP server start")
	}
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

	if ok := p.startMonitoringServer(); ok {
		t.Fatal("expected HTTP server start to fail when the port is already in use")
	}
}

func TestProxy_startHTTP_BlankHost_Bad(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Enabled: true,
				Port:    0,
			},
		},
		done: make(chan struct{}),
	}

	if ok := p.startMonitoringServer(); ok {
		t.Fatal("expected HTTP server start to fail with an empty host")
	}
}

func TestProxy_registerMonitoringRoute_MethodNotAllowed_Bad(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Restricted: true,
			},
		},
	}

	mux := http.NewServeMux()
	p.registerMonitoringRoute(mux, "/1/summary", func() any { return map[string]string{"status": "ok"} })

	request := httptest.NewRequest(http.MethodPost, "/1/summary", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}

func TestProxy_registerMonitoringRoute_Unauthorized_Ugly(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				AccessToken: "secret",
			},
		},
	}

	mux := http.NewServeMux()
	p.registerMonitoringRoute(mux, "/1/summary", func() any { return map[string]string{"status": "ok"} })

	request := httptest.NewRequest(http.MethodGet, "/1/summary", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
	if got := recorder.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("expected WWW-Authenticate header %q, got %q", "Bearer", got)
	}
}
