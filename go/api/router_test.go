package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"dappco.re/go/proxy"
	"github.com/gin-gonic/gin"
)

type testRouter struct {
	engine *gin.Engine
}

func newTestRouter() (*testRouter, error) {
	gin.SetMode(gin.TestMode)
	return &testRouter{engine: gin.New()}, nil
}

func (router *testRouter) Register(group RouteGroup) {
	if router == nil || group == nil {
		return
	}
	group.RegisterRoutes(router.engine.Group(group.BasePath()))
}

func (router *testRouter) Handler() http.Handler {
	return router.engine
}

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

	router, err := newTestRouter()
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
	// target symbol: AllowMonitoringRequest
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
	// target symbol: GETSummary
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

	router, err := newTestRouter()
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
	if err := testJSONUnmarshal(recorder.Body.Bytes(), &document); err != nil {
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
	// target symbol: GETWorkers
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

	router, err := newTestRouter()
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
	if err := testJSONUnmarshal(recorder.Body.Bytes(), &document); err != nil {
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
	// target symbol: POSTSummary
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

	router, err := newTestRouter()
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
	// target tokens: POSTSummary Unrestricted
	// target symbol: POSTSummary_Unrestricted
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

	router, err := newTestRouter()
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
	// target symbol: POSTWorkers
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

	router, err := newTestRouter()
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

func TestRegisterRoutes_PUTSummary_Bad(t *testing.T) {
	// target symbol: PUTSummary
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

	router, err := newTestRouter()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	handler := router.Handler()

	request := httptest.NewRequest(http.MethodPut, "/1/summary", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}

func TestRegisterRoutes_GETMiners_Ugly(t *testing.T) {
	// target symbol: GETMiners
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

	router, err := newTestRouter()
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
	if err := testJSONUnmarshal(recorder.Body.Bytes(), &document); err != nil {
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
	// target symbol: NameAndBasePath
	routes := &monitoringRoutes{}

	if got := routes.Name(); got != "proxy-monitoring" {
		t.Fatalf("expected route name %q, got %q", "proxy-monitoring", got)
	}
	if got := routes.BasePath(); got != "/1" {
		t.Fatalf("expected base path %q, got %q", "/1", got)
	}
}

func TestRegisterRoutes_GETSummaryAuthRequired_Bad(t *testing.T) {
	// target symbol: GETSummaryAuthRequired
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

	router, err := newTestRouter()
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
	// target symbol: GETSummaryAuthGranted
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

	router, err := newTestRouter()
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
	// target symbol: GETWorkersAuthRequired
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

	router, err := newTestRouter()
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

func TestRouter_RegisterRoutes_Good(t *testing.T) {
	p, result := proxy.New(&proxy.Config{Mode: "simple", Workers: proxy.WorkersByRigID, Bind: []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}}, Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}
	router, err := newTestRouter()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, p)
	recorder := httptest.NewRecorder()
	router.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/1/summary", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected registered summary route, got %d", recorder.Code)
	}
}

func TestRouter_RegisterRoutes_Bad(t *testing.T) {
	router, err := newTestRouter()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	RegisterRoutes(router, nil)
	recorder := httptest.NewRecorder()
	router.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/1/summary", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected nil proxy registration ignored, got %d", recorder.Code)
	}
}

func TestRouter_RegisterRoutes_Ugly(t *testing.T) {
	p, result := proxy.New(&proxy.Config{Mode: "simple", Workers: proxy.WorkersByRigID, Bind: []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}}, Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}
	RegisterRoutes(nil, p)
	if p.Mode() != "simple" {
		t.Fatalf("expected nil router call not to mutate proxy, mode=%q", p.Mode())
	}
}

func TestRouter_Routes_Name_Good(t *testing.T) {
	routes := &monitoringRoutes{}
	if got := routes.Name(); got != "proxy-monitoring" {
		t.Fatalf("expected route name, got %q", got)
	}
}

func TestRouter_Routes_Name_Bad(t *testing.T) {
	var routes *monitoringRoutes
	if got := routes.Name(); got != "proxy-monitoring" {
		t.Fatalf("expected nil route name to be stable, got %q", got)
	}
}

func TestRouter_Routes_Name_Ugly(t *testing.T) {
	routes := &monitoringRoutes{proxy: nil}
	if got := routes.Name(); got != "proxy-monitoring" {
		t.Fatalf("expected route name independent of proxy, got %q", got)
	}
}

func TestRouter_Routes_BasePath_Good(t *testing.T) {
	routes := &monitoringRoutes{}
	if got := routes.BasePath(); got != "/1" {
		t.Fatalf("expected base path /1, got %q", got)
	}
}

func TestRouter_Routes_BasePath_Bad(t *testing.T) {
	var routes *monitoringRoutes
	if got := routes.BasePath(); got != "/1" {
		t.Fatalf("expected nil route base path to be stable, got %q", got)
	}
}

func TestRouter_Routes_BasePath_Ugly(t *testing.T) {
	routes := &monitoringRoutes{proxy: nil}
	if got := routes.BasePath(); got != "/1" {
		t.Fatalf("expected base path independent of proxy, got %q", got)
	}
}

func TestRouter_Routes_RegisterRoutes_Good(t *testing.T) {
	// target tokens: Routes RegisterRoutes
	// target symbol: Routes_RegisterRoutes
	p, result := proxy.New(&proxy.Config{Mode: "simple", Workers: proxy.WorkersByRigID, Bind: []proxy.BindAddr{{Host: "127.0.0.1", Port: 3333}}, Pools: []proxy.PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}
	router, err := newTestRouter()
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	router.Register(&monitoringRoutes{proxy: p})
	recorder := httptest.NewRecorder()
	router.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/1/miners", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected registered miners route, got %d", recorder.Code)
	}
}

func TestRouter_Routes_RegisterRoutes_Bad(t *testing.T) {
	var routes *monitoringRoutes
	routes.RegisterRoutes(nil)
	if routes != nil {
		t.Fatal("expected nil routes to remain nil")
	}
}

func TestRouter_Routes_RegisterRoutes_Ugly(t *testing.T) {
	routes := &monitoringRoutes{}
	routes.RegisterRoutes(nil)
	if routes.proxy != nil {
		t.Fatalf("expected nil group call not to mutate routes, got %+v", routes)
	}
}
