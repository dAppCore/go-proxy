// SPDX-License-Identifier: EUPL-1.2

package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	core "dappco.re/go"
)

// serviceTestConfig returns a minimal proxy Config that binds an ephemeral
// loopback port so Service.Start can run the real proxy without contending
// for a fixed port.
func serviceTestConfig() *Config {
	return &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 0}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
}

// TestNewService_ValidConfig_ConstructsProxy verifies the happy path —
// NewService with a valid config constructs the underlying *Proxy so the
// registered Service is ready to Start.
func TestNewService_ValidConfig_ConstructsProxy(t *testing.T) {
	c := core.New(core.WithService(NewService(serviceTestConfig())))
	r := c.Service("proxy")
	if !r.OK {
		t.Fatalf("proxy service not registered, got %#v", r.Value)
	}
	svc := r.Value.(*Service)
	if svc.Proxy == nil {
		t.Fatal("expected NewService with valid config to construct a Proxy")
	}
}

// TestNewService_InvalidConfig_Fails verifies that a config which fails
// proxy construction surfaces as a failed Result rather than a partly-built
// service.
func TestNewService_InvalidConfig_Fails(t *testing.T) {
	bad := &Config{
		Mode:    "simple",
		Workers: WorkersByRigID,
		// No pools, no bind — New() rejects an unusable config.
	}
	r := NewService(bad)(core.New())
	if r.OK {
		t.Fatalf("expected NewService with invalid config to fail, got %#v", r.Value)
	}
}

// TestService_Start_ContextCancel_StopsProxy verifies the full lifecycle:
// Start launches the blocking Proxy in a goroutine, the listener comes up,
// and cancelling the supplied context triggers Proxy.Stop so the listener
// closes.
func TestService_Start_ContextCancel_StopsProxy(t *testing.T) {
	c := core.New(core.WithService(NewService(serviceTestConfig())))
	svc := c.Service("proxy").Value.(*Service)

	ctx, cancel := context.WithCancel(t.Context())
	if r := svc.Start(ctx); !r.OK {
		t.Fatalf("expected Start to succeed, got %v", r.Error())
	}

	// Wait for the listener to come up so we know Proxy.Start ran.
	deadline := time.Now().Add(2 * time.Second)
	var addr string
	for time.Now().Before(deadline) {
		if addr = svc.Proxy.ServerListenerAddr(0); addr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		cancel()
		t.Fatal("expected proxy listener to come up after Start")
	}

	// Cancelling the context must drive Proxy.Stop via Service's watcher
	// goroutine; once shutdown completes the listener stops accepting, so a
	// fresh dial to the captured address fails.
	cancel()
	stopDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(stopDeadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected listener to stop accepting after context cancel")
}

// TestService_Start_NilContext_DoesNotPanic verifies Start tolerates a nil
// context — the proxy runs but no watcher goroutine is wired, so the caller
// owns shutdown via Stop.
func TestService_Start_NilContext_DoesNotPanic(t *testing.T) {
	c := core.New(core.WithService(NewService(serviceTestConfig())))
	svc := c.Service("proxy").Value.(*Service)

	//nolint:staticcheck // intentionally passing a nil context to exercise the guard
	if r := svc.Start(nil); !r.OK {
		t.Fatalf("expected Start with nil context to succeed, got %v", r.Error())
	}
	if r := svc.Stop(); !r.OK {
		t.Fatalf("expected Stop to succeed, got %v", r.Error())
	}
}

// TestNewService_NilConfig_RegistersWithoutProxy verifies the lazy-injection
// shape — passing nil config registers the Service handle with a nil Proxy.
// Start/Stop on a nil-Proxy service must return a configuration error rather
// than panic.
func TestNewService_NilConfig_RegistersWithoutProxy(t *testing.T) {
	c := core.New(core.WithService(NewService(nil)))
	r := c.Service("proxy")
	if !r.OK {
		t.Fatal("proxy service not registered via NewService(nil)")
	}
	svc := r.Value.(*Service)
	if svc.Proxy != nil {
		t.Fatalf("expected nil Proxy with nil config, got %#v", svc.Proxy)
	}

	if start := svc.Start(t.Context()); start.OK {
		t.Fatal("expected nil-Proxy Start to fail")
	}
	if stop := svc.Stop(); stop.OK {
		t.Fatal("expected nil-Proxy Stop to fail")
	}
}

// TestRegister_DefaultsRegistersWithoutProxy verifies the imperative-style
// Register(c) shorthand registers the Service with default (nil) config.
func TestRegister_DefaultsRegistersWithoutProxy(t *testing.T) {
	c := core.New(core.WithService(Register))
	r := c.Service("proxy")
	if !r.OK {
		t.Fatalf("proxy service not registered via Register, got %#v", r.Value)
	}
	svc := r.Value.(*Service)
	if svc.Proxy != nil {
		t.Fatal("expected nil Proxy with default Register")
	}
}

// TestService_NilReceiver_GuardsStartStop verifies the nil-receiver guards
// in Start / Stop don't panic — defensive for callers that hold a nil *Service
// pointer.
func TestService_NilReceiver_GuardsStartStop(t *testing.T) {
	var svc *Service
	if r := svc.Start(t.Context()); r.OK {
		t.Fatal("expected nil-receiver Start to fail")
	}
	if r := svc.Stop(); r.OK {
		t.Fatal("expected nil-receiver Stop to fail")
	}
}
