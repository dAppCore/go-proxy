package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	coreapi "dappco.re/go/core/api"
	"dappco.re/go/proxy"
)

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

func TestRegisterRoutes_POSTSummary_Unrestricted_Good(t *testing.T) {
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

func TestRegisterRoutes_GETSummaryAuthRequired_Bad(t *testing.T) {
	config := &proxy.Config{
		Mode:    "nicehash",
		Workers: proxy.WorkersByRigID,
		Bind:    []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}},
		Pools:   []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}},
		HTTP: proxy.HTTPConfig{
			Enabled:     true,
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
