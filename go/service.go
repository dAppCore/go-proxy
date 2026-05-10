// SPDX-License-Identifier: EUPL-1.2

// Service registration for the proxy package — exposes the canonical
// `NewService(config)` + `Register(c)` shape per Mantis #1336, wrapping
// the existing `New(*Config) (*Proxy, Result)` constructor in a
// Core-registerable factory.
//
//	c, _ := core.New(
//	    core.WithService(proxy.NewService(myConfig)),
//	)
//	svc := core.MustServiceFor[*proxy.Service](c, "proxy")
//	r := svc.Start(ctx)
//
// The *Proxy type does the heavy lifting — Service is a thin Core-bound
// handle holding the *Proxy plus typed config-options access via
// *core.ServiceRuntime. Proxy.Start() is blocking; Service.Start runs
// it in a goroutine and tracks lifecycle via the supplied context, so
// callers can compose proxy alongside other Core services without
// dedicating a goroutine themselves.

package proxy

import (
	"context"

	core "dappco.re/go"
)

// Service is the registerable handle for the proxy package — embeds
// *core.ServiceRuntime[*Config] for typed options access and holds a
// constructed *Proxy ready for Start / Stop.
//
// Usage example: `svc := core.MustServiceFor[*proxy.Service](c, "proxy"); r := svc.Start(ctx)`
type Service struct {
	*core.ServiceRuntime[*Config]
	// Proxy is the live proxy instance the service was constructed with.
	// nil if NewService was called with a nil config (the Service is
	// registered but Start/Stop will return a configuration error).
	Proxy *Proxy
}

// NewService returns a factory that constructs a *Proxy from the given
// config and wraps it as a Core-registerable *Service. Passing nil
// config registers a service with a nil Proxy — Start/Stop on that
// service will return an error rather than panic, so callers that
// boot proxy lazily (e.g. config-after-registration) can do so.
//
//	core.WithService(proxy.NewService(myConfig))
func NewService(config *Config) func(*core.Core) core.Result {
	return func(c *core.Core) core.Result {
		svc := &Service{
			ServiceRuntime: core.NewServiceRuntime(c, config),
		}
		if config == nil {
			return core.Ok(svc)
		}
		p, result := New(config)
		if !result.OK {
			return core.Fail(core.E("proxy.NewService", "proxy construction failed", result.Error))
		}
		svc.Proxy = p
		return core.Ok(svc)
	}
}

// Register wires the proxy service into the Core with a nil config —
// the imperative-style alternative to NewService for consumers that
// want the service handle registered but will inject Proxy later.
//
//	c := core.New()
//	if r := proxy.Register(c); !r.OK { return r }
//	svc := core.MustServiceFor[*proxy.Service](c, "proxy")
//	p, _ := proxy.New(myConfig)
//	svc.Proxy = p
//	svc.Start(ctx)
func Register(c *core.Core) core.Result {
	return NewService(nil)(c)
}

// Start delegates to the underlying *Proxy. Proxy.Start() is blocking;
// Service.Start runs it in a goroutine and calls Stop() when ctx is
// cancelled. Returns a configuration error if the service was
// registered with a nil Proxy.
//
//	r := svc.Start(ctx)
func (s *Service) Start(ctx context.Context) core.Result {
	if s == nil || s.Proxy == nil {
		return core.Fail(core.E("proxy.Service.Start", "proxy not configured", nil))
	}
	p := s.Proxy
	go p.Start()
	if ctx != nil {
		go func() {
			<-ctx.Done()
			p.Stop()
		}()
	}
	return core.Ok(nil)
}

// Stop delegates to the underlying *Proxy. Returns a configuration
// error if the service was registered with a nil Proxy.
//
//	r := svc.Stop()
func (s *Service) Stop() core.Result {
	if s == nil || s.Proxy == nil {
		return core.Fail(core.E("proxy.Service.Stop", "proxy not configured", nil))
	}
	s.Proxy.Stop()
	return core.Ok(nil)
}
