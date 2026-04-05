// Package api implements the HTTP monitoring endpoints for the proxy.
//
// mux := http.NewServeMux()
// RegisterRoutes(mux, p)
package api

import (
	"encoding/json"
	"net/http"

	"dappco.re/go/proxy"
)

// http.NewServeMux()
type Router interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// mux := http.NewServeMux()
// RegisterRoutes(mux, p)
func RegisterRoutes(router Router, p *proxy.Proxy) {
	if router == nil || p == nil {
		return
	}
	registerGETRoute(router, "/1/summary", func() any { return p.SummaryDocument() })
	registerGETRoute(router, "/1/workers", func() any { return p.WorkersDocument() })
	registerGETRoute(router, "/1/miners", func() any { return p.MinersDocument() })
}

func registerGETRoute(router Router, pattern string, document func() any) {
	router.HandleFunc(pattern, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, document())
	})
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
