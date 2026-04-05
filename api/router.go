// Package api registers the monitoring routes on an HTTP mux.
//
//	mux := http.NewServeMux()
//	api.RegisterRoutes(mux, p)
package api

import (
	"encoding/json"
	"net/http"

	"dappco.re/go/proxy"
)

type Router interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// mux := http.NewServeMux()
// api.RegisterRoutes(mux, p) // GET /1/summary, /1/workers, /1/miners
func RegisterRoutes(router Router, p *proxy.Proxy) {
	if router == nil || p == nil {
		return
	}
	registerJSONGetRoute(router, "/1/summary", func() any { return p.SummaryDocument() })
	registerJSONGetRoute(router, "/1/workers", func() any { return p.WorkersDocument() })
	registerJSONGetRoute(router, "/1/miners", func() any { return p.MinersDocument() })
}

func registerJSONGetRoute(router Router, pattern string, renderDocument func() any) {
	router.HandleFunc(pattern, func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, renderDocument())
	})
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
