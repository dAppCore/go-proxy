package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	coreapi "dappco.re/go/core/api"
	"dappco.re/go/proxy"
)

func TestApiRouter_RegisterRoutes_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	RegisterRoutes(nil, p)

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, nil)
	handler := router.Handler()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/1/summary", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected unregistered route to return %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestApiRouter_AllowMonitoringRequest_Bad(t *testing.T) {
	status, ok := allowMonitoringRequest(nil, httptest.NewRequest("GET", "/1/summary", nil))
	if ok {
		t.Fatal("expected nil proxy to be rejected")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, status)
	}
}

func TestApiRouter_MonitoringRoutes_RegisterRoutes_Ugly(t *testing.T) {
	var routes *monitoringRoutes
	routes.RegisterRoutes(nil)

	routes = &monitoringRoutes{}
	routes.RegisterRoutes(nil)
}

func TestRegisterRoutes_GETSummary_Good(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/summary", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, recorder.Code)
	}

	var document proxy.SummaryDocument
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode summary document: %v", err)
	}
	if document.Mode != "nicehash" {
		t.Fatalf("expected mode %q, got %q", "nicehash", document.Mode)
	}
	if document.Version != "1.0.0" {
		t.Fatalf("expected version %q, got %q", "1.0.0", document.Version)
	}
}

func TestRegisterRoutes_GETWorkers_Good(t *testing.T) {
	config := &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/workers", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, recorder.Code)
	}

	var document proxy.WorkersDocument
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode workers document: %v", err)
	}
	if document.Mode != string(proxy.WorkersByRigID) {
		t.Fatalf("expected workers mode %q, got %q", proxy.WorkersByRigID, document.Mode)
	}
	if len(document.Workers) != 0 {
		t.Fatalf("expected no workers in a new proxy, got %d", len(document.Workers))
	}
}

func TestRegisterRoutes_POSTSummary_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Restricted: true,
		},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("POST", "/1/summary", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestRegisterRoutes_POSTSummary_Unrestricted_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("POST", "/1/summary", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestRegisterRoutes_POSTWorkers_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Restricted: true,
		},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("POST", "/1/workers", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestRegisterRoutes_GETMiners_Ugly(t *testing.T) {
	config := &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersDisabled,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/miners", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, recorder.Code)
	}

	var document proxy.MinersDocument
	if err := json.Unmarshal(recorder.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode miners document: %v", err)
	}
	if len(document.Format) != 10 {
		t.Fatalf("expected 10 miner columns, got %d", len(document.Format))
	}
	if len(document.Miners) != 0 {
		t.Fatalf("expected no miners in a new proxy, got %d", len(document.Miners))
	}
}

func TestMonitoringRoutes_NameAndBasePath_Good(t *testing.T) {
	routes := &monitoringRoutes{}

	if got := routes.Name(); got != "proxy-monitoring" {
		t.Fatalf("expected route name %q, got %q", "proxy-monitoring", got)
	}
	if got := routes.BasePath(); got != "/1" {
		t.Fatalf("expected base path %q, got %q", "/1", got)
	}
}

func TestRegisterRoutes_GETSummaryAuthRequired_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Enabled:     true,
			Host:        "127.0.0.1",
			Restricted:  true,
			AccessToken: "secret",
		},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/summary", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
	if got := recorder.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("expected bearer challenge, got %q", got)
	}
}

func TestRegisterRoutes_GETSummaryAuthGranted_Ugly(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Enabled:     true,
			Host:        "127.0.0.1",
			Restricted:  true,
			AccessToken: "secret",
		},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/summary", nil)
	request.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, recorder.Code)
	}
}

func TestRegisterRoutes_GETWorkersAuthRequired_Ugly(t *testing.T) {
	config := &proxy.Config{
		Mode:    "simple",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Enabled:     true,
			Host:        "127.0.0.1",
			Restricted:  true,
			AccessToken: "secret",
		},
	}
	p, result := proxy.New(config)
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}

	router, err := coreapi.New()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest("GET", "/1/workers", nil)
	request.Header.Set("Authorization", "Bearer wrong")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
	if got := recorder.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("expected bearer challenge, got %q", got)
	}
}
