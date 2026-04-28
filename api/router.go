// Package api mounts the monitoring endpoints on a Core API engine.
//
//	engine, _ := coreapi.New()
//	api.RegisterRoutes(engine, proxyInstance)
package api

import (
	"net/http"

	core "dappco.re/go"
	coreapi "dappco.re/go/api"
	"dappco.re/go/proxy"
	"github.com/gin-gonic/gin"
)

type monitoringRoutes struct {
	proxy *proxy.Proxy
}

// api.RegisterRoutes(engine, proxyInstance)
//
// The mounted routes are GET /1/summary, /1/workers, and /1/miners.
func RegisterRoutes(router *coreapi.Engine, p *proxy.Proxy) {
	if router == nil || p == nil {
		return
	}
	router.Register(&monitoringRoutes{proxy: p})
}

func (routes *monitoringRoutes) Name() string {
	return "proxy-monitoring"
}

func (routes *monitoringRoutes) BasePath() string {
	return "/1"
}

func (routes *monitoringRoutes) RegisterRoutes(group *gin.RouterGroup) {
	if routes == nil || routes.proxy == nil || group == nil {
		return
	}
	registerJSONRoute(group, routes.proxy, "/summary", func() any { return routes.proxy.SummaryDocument() })
	registerJSONRoute(group, routes.proxy, "/workers", func() any { return routes.proxy.WorkersDocument() })
	registerJSONRoute(group, routes.proxy, "/miners", func() any { return routes.proxy.MinersDocument() })
}

// registerJSONRoute(group, proxyInstance, "/summary", func() any { return proxyInstance.SummaryDocument() })
//
// POST is routed to the same handler so non-GET requests get a consistent 405
// response with an Allow header at the API boundary.
func registerJSONRoute(group *gin.RouterGroup, proxyInstance *proxy.Proxy, path string, renderDocument func() any) {
	handler := func(context *gin.Context) {
		if status, ok := allowMonitoringRequest(proxyInstance, context.Request); !ok {
			switch status {
			case http.StatusMethodNotAllowed:
				context.Header("Allow", http.MethodGet)
			case http.StatusUnauthorized:
				context.Header("WWW-Authenticate", "Bearer")
			}
			context.Status(status)
			return
		}
		writeJSON(context.Writer, renderDocument())
	}
	group.Any(path, handler)
}

func allowMonitoringRequest(proxyInstance *proxy.Proxy, request *http.Request) (int, bool) {
	if proxyInstance == nil {
		return http.StatusServiceUnavailable, false
	}
	return proxyInstance.AllowMonitoringRequest(request)
}

func writeJSON(writer http.ResponseWriter, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(core.JSONMarshalString(payload) + "\n"))
}
