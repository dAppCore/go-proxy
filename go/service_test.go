// SPDX-License-Identifier: EUPL-1.2

package proxy

import (
	"context"
	"testing"

	core "dappco.re/go"
)

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

	if start := svc.Start(context.Background()); start.OK {
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
	if r := svc.Start(context.Background()); r.OK {
		t.Fatal("expected nil-receiver Start to fail")
	}
	if r := svc.Stop(); r.OK {
		t.Fatal("expected nil-receiver Stop to fail")
	}
}
