module dappco.re/go/proxy

go 1.26.0

require (
	dappco.re/go/core v0.0.0
	dappco.re/go/core/api v0.0.0
	github.com/gin-gonic/gin v1.11.0
)

replace dappco.re/go/core => ../go

replace dappco.re/go/core/api => ../api

replace dappco.re/go/core/io => ../go-io

replace dappco.re/go/core/log => ../go-log
