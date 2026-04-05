// Package api mounts the monitoring endpoints on an HTTP mux.
//
//	mux := http.NewServeMux()
//	api.RegisterRoutes(mux, p)
package api

import (
	"encoding/json"
	"net/http"

	"dappco.re/go/proxy"
)

// RouteRegistrar accepts HTTP handler registrations.
//
//	mux := http.NewServeMux()
//	api.RegisterRoutes(mux, p)
type RouteRegistrar interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// mux := http.NewServeMux()
// api.RegisterRoutes(mux, p)
// GET /1/summary, /1/workers, and /1/miners
func RegisterRoutes(router RouteRegistrar, p *proxy.Proxy) {
	if router == nil || p == nil {
		return
	}
	registerJSONGetRoute(router, p, "/1/summary", func() any { return p.SummaryDocument() })
	registerJSONGetRoute(router, p, "/1/workers", func() any { return p.WorkersDocument() })
	registerJSONGetRoute(router, p, "/1/miners", func() any { return p.MinersDocument() })
}

func registerJSONGetRoute(router RouteRegistrar, authoriser *proxy.Proxy, pattern string, renderDocument func() any) {
	router.HandleFunc(pattern, func(w http.ResponseWriter, request *http.Request) {
		if status, ok := allowMonitoringRequest(authoriser, request); !ok {
			switch status {
			case http.StatusMethodNotAllowed:
				w.Header().Set("Allow", http.MethodGet)
			case http.StatusUnauthorized:
				w.Header().Set("WWW-Authenticate", "Bearer")
			}
			w.WriteHeader(status)
			return
		}
		writeJSON(w, renderDocument())
	})
}

func allowMonitoringRequest(authoriser *proxy.Proxy, request *http.Request) (int, bool) {
	if request.Method != http.MethodGet {
		return http.StatusMethodNotAllowed, false
	}
	if authoriser == nil {
		return http.StatusServiceUnavailable, false
	}
	return authoriser.AllowMonitoringRequest(request)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
