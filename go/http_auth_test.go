package proxy

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestProxy_allowHTTP_Good(t *testing.T) {
	// target symbol: allowHTTP
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
	// target symbol: allowHTTP
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
	// target tokens: allowHTTP Unrestricted
	// target symbol: allowHTTP_Unrestricted
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
	// target tokens: allowHTTP Unrestricted
	// target symbol: allowHTTP_Unrestricted
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{},
		},
	}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodPost})
	if ok {
		t.Fatal("expected monitoring POST request to be rejected")
	}
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestProxy_allowHTTP_Ugly(t *testing.T) {
	// target symbol: allowHTTP
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

func TestProxy_allowHTTP_PublicHostWithoutToken_Bad(t *testing.T) {
	// target tokens: allowHTTP PublicHostWithoutToken
	// target symbol: allowHTTP_PublicHostWithoutToken
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Host: "0.0.0.0",
			},
		},
	}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if ok {
		t.Fatal("expected public monitoring without token to be rejected")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, status)
	}
}

func TestProxy_allowHTTP_MalformedAuth_Bad(t *testing.T) {
	// target tokens: allowHTTP MalformedAuth
	// target symbol: allowHTTP_MalformedAuth
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
			"Authorization": []string{"Bearer"},
		},
	})
	if ok {
		t.Fatal("expected malformed authorization header to be rejected")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, status)
	}
}

func TestProxy_allowHTTP_LowercaseBearer_Good(t *testing.T) {
	// target tokens: allowHTTP LowercaseBearer
	// target symbol: allowHTTP_LowercaseBearer
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
			"Authorization": []string{"bearer secret"},
		},
	})
	if !ok {
		t.Fatalf("expected lowercase bearer scheme to be accepted, got status %d", status)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, status)
	}
}

func TestProxy_allowHTTP_NilConfig_Ugly(t *testing.T) {
	// target tokens: allowHTTP NilConfig
	// target symbol: allowHTTP_NilConfig
	p := &Proxy{}

	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if ok {
		t.Fatal("expected nil config request to be rejected")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, status)
	}
}

func TestProxy_allowHTTP_NilRequest_Bad(t *testing.T) {
	// target tokens: allowHTTP NilRequest
	// target symbol: allowHTTP_NilRequest
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{Enabled: true, AccessToken: "secret"},
		},
	}

	status, ok := p.AllowMonitoringRequest(nil)
	if ok {
		t.Fatal("expected nil request to be rejected")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, status)
	}
}

func TestProxy_secureStringEqual_Good(t *testing.T) {
	if !secureStringEqual("secret", "secret") {
		t.Fatal("expected identical strings to compare equal")
	}
}

func TestProxy_secureStringEqual_Bad(t *testing.T) {
	if secureStringEqual("secret", "secrex") {
		t.Fatal("expected different same-length strings to compare unequal")
	}
}

func TestProxy_secureStringEqual_Ugly(t *testing.T) {
	if secureStringEqual("secret", "") {
		t.Fatal("expected strings with different lengths to compare unequal")
	}
}

func TestProxy_startHTTP_Good(t *testing.T) {
	// target symbol: startHTTP
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
	if httpServer.MaxHeaderBytes == 0 {
		t.Fatal("expected monitoring server to cap header bytes")
	}
	if httpServer.MaxHeaderBytes != httpMaxHeaderBytes {
		t.Fatalf("expected monitoring server header limit %d, got %d", httpMaxHeaderBytes, httpServer.MaxHeaderBytes)
	}
	p.Stop()
}

func TestProxy_startHTTP_NilConfig_Bad(t *testing.T) {
	// target tokens: startHTTP NilConfig
	// target symbol: startHTTP_NilConfig
	p := &Proxy{}

	if ok := p.startMonitoringServer(); ok {
		t.Fatal("expected nil config to skip HTTP server start")
	}
}

func TestProxy_startHTTP_Bad(t *testing.T) {
	// target symbol: startHTTP
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

func TestProxy_registerMonitoringRoute_NilInputs_Ugly(t *testing.T) {
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{Enabled: true, AccessToken: "secret"},
		},
	}

	p.registerMonitoringRoute(nil, MonitoringRouteSummary, func() any { return nil })
	p.registerMonitoringRoute(http.NewServeMux(), MonitoringRouteSummary, nil)
}

func TestProxy_startHTTP_BlankHost_Bad(t *testing.T) {
	// target tokens: startHTTP BlankHost
	// target symbol: startHTTP_BlankHost
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

func TestProxy_startHTTP_PublicHostWithoutToken_Bad(t *testing.T) {
	// target tokens: startHTTP PublicHostWithoutToken
	// target symbol: startHTTP_PublicHostWithoutToken
	p := &Proxy{
		config: &Config{
			HTTP: HTTPConfig{
				Enabled: true,
				Host:    "0.0.0.0",
				Port:    0,
			},
		},
		done: make(chan struct{}),
	}

	if ok := p.startMonitoringServer(); ok {
		t.Fatal("expected HTTP server start to fail without a token on a public host")
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
