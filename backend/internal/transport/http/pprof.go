package httpserver

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/chenyme/grok2api/backend/internal/application/gateway"
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

// registerCacheStats mounts routing-cache hit/miss counters under /debug/cache/stats.
// Same enablement gate as pprof: unauthenticated, debug-only.
// stats should return gateway.RoutingCacheStats or a map with assembled/base/overlay keys.
func registerCacheStats(router *gin.Engine, stats func() any, reset func()) {
	if stats == nil {
		return
	}
	router.GET("/debug/cache/stats", func(c *gin.Context) {
		snapshot := stats()
		body := gin.H{
			"enabled":      true,
			"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		}
		switch value := snapshot.(type) {
		case gateway.RoutingCacheStats:
			body["layers"] = gin.H{
				"assembled": value.Assembled,
				"base":      value.Base,
				"overlay":   value.Overlay,
			}
			body["invalidation"] = value.Invalidation
			body["sizes"] = value.Sizes
		default:
			// Test doubles may return a pre-shaped layers map.
			body["layers"] = snapshot
		}
		c.JSON(http.StatusOK, body)
	})
	if reset != nil {
		// POST only: mutating GET is too easy to trigger via prefetch/crawlers.
		router.POST("/debug/cache/stats/reset", func(c *gin.Context) {
			reset()
			c.JSON(http.StatusOK, gin.H{"ok": true})
		})
	}
}
