package httpserver

import (
	"net/http/pprof"
	"runtime"

	"github.com/gin-gonic/gin"
)

// registerPprof mounts Go runtime profiles under /debug/pprof.
// Callers must gate this behind an explicit config flag; the handlers are unauthenticated.
func registerPprof(router *gin.Engine) {
	// Enable contention profiles while pprof is on so /block and /mutex are useful.
	// Rates stay modest to limit steady-state overhead on small hosts.
	runtime.SetBlockProfileRate(10_000)
	runtime.SetMutexProfileFraction(5)

	group := router.Group("/debug/pprof")
	group.GET("/", gin.WrapF(pprof.Index))
	group.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	group.GET("/profile", gin.WrapF(pprof.Profile))
	group.GET("/symbol", gin.WrapF(pprof.Symbol))
	group.POST("/symbol", gin.WrapF(pprof.Symbol))
	group.GET("/trace", gin.WrapF(pprof.Trace))
	for _, name := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		group.GET("/"+name, gin.WrapH(pprof.Handler(name)))
	}
}
